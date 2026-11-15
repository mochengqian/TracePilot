"""A bounded decision loop. Model outputs never bypass runtime validation."""

import copy
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context
from datetime import timedelta
from typing import Any, Callable
from uuid import uuid4

from jsonschema import Draft202012Validator
from tenacity import RetryCallState, Retrying, retry, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_random_exponential

from tracepilot.errors import TRANSIENT_TOOL_ERRORS
from tracepilot.models import (
    ActionGroup, ActionRecord, Candidate, DecisionState, Diagnosis, DiagnosisDraft, Evidence,
    InvestigationResult, TerminalStatus, ToolOutput, ToolSpec, utc_now,
)
from tracepilot.policy import available_actions, can_attempt, can_reason
from tracepilot.providers import DecisionProvider, Reasoner
from tracepilot.reporting import render_diagnosis
from tracepilot.store import EvidenceStore
from tracepilot.gateway import ToolGateway, tool_error_output
from tracepilot.validation import validator
from tracepilot.limits import DeadlineExceeded, check_retry_deadline, request_deadline


READ_EVIDENCE = ToolSpec(
    name="agent_read_evidence", source="decision-agent", retry_safe=True,
    description="Read a page of a previously collected raw tool result in this task by evidence ID.",
    parameters={
        "type": "object", "additionalProperties": False,
        "properties": {
            "evidence_id": {"type": "string"},
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 1, "maximum": 12000},
        },
        "required": ["evidence_id", "offset", "limit"],
    },
)


class DecisionAgent:
    def __init__(
        self, reasoner: Reasoner, decision_provider: DecisionProvider,
        tools: ToolGateway, store: EvidenceStore,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        retry_wait: Callable[[RetryCallState], float] | None = None,
    ):
        self.reasoner = reasoner
        self.decision_provider = decision_provider
        self.tools = tools
        self.store = store
        self.on_event = on_event
        self.clock = clock
        self.sleep = sleep
        self.retry_wait = retry_wait or wait_random_exponential(multiplier=0.5, max=4)
        self._usage_offsets: dict[int, int] = {}

    def _save(
        self, state: DecisionState, kind: str, *, evidence: Evidence | None = None,
        diagnosis: Diagnosis | None = None, **details: Any,
    ) -> None:
        event = {"kind": kind, "task_id": state.task_id, **details}
        self.store.checkpoint(state, event=event, evidence=evidence, diagnosis=diagnosis)
        if self.on_event:
            self.on_event(event)

    @retry(retry=retry_if_exception_type((TimeoutError, ConnectionError)), stop=stop_after_attempt(2),
           wait=wait_random_exponential(multiplier=0.2, max=1), before_sleep=check_retry_deadline, reraise=True)
    def _refresh(self, state: DecisionState) -> dict[str, ToolSpec]:
        catalog = self.tools.catalog(refresh=True)
        names = [tool.name for tool in catalog]
        if len(names) != len(set(names)) or READ_EVIDENCE.name in names:
            raise ValueError("Tool names must be unique; agent_read_evidence is reserved")
        if len(catalog) > 100:
            raise ValueError("Too many tools; narrow the session with --allow-tool")
        state.available_tools = [*catalog, READ_EVIDENCE]
        return {tool.name: tool for tool in state.available_tools}

    def _validate_candidate(self, candidate: Candidate, tools: dict[str, ToolSpec]) -> None:
        spec = tools.get(candidate.tool)
        if spec is None:
            raise ValueError(f"Unavailable tool: {candidate.tool}")

        def check_refs(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in {"$ref", "$dynamicRef"} and isinstance(child, str) and not child.startswith("#"):
                        raise ValueError("External schema references are unsupported")
                    check_refs(child)
            elif isinstance(value, list):
                for child in value:
                    check_refs(child)

        check_refs(spec.parameters)
        Draft202012Validator.check_schema(spec.parameters)
        Draft202012Validator(spec.parameters).validate(candidate.arguments)
        check_scope = getattr(self.tools, "validate_candidate", None)
        if candidate.tool != READ_EVIDENCE.name and callable(check_scope):
            check_scope(candidate)

    @staticmethod
    def _observation_ids(state: DecisionState) -> set[str]:
        return {e["id"] for e in state.evidence
                if e["status"] in {"success", "no_data"} and e["tool"] != READ_EVIDENCE.name}

    def _reason(self, state: DecisionState, catalog: dict[str, ToolSpec]) -> None:
        if not can_reason(state):
            raise ValueError("Reasoning is not an available action")
        state.reasoning_calls += 1
        previous = {candidate.fingerprint for candidate in state.candidates}
        self._save(state, "reasoning_started")
        try:
            try:
                result = self.reasoner.reason(state.model_copy(deep=True))
            finally:
                self._usage(state, getattr(self.reasoner, "llm", None), "reason")
            result.memory.validate_references(self._observation_ids(state))
            ids = [candidate.id for candidate in result.candidates]
            if len(set(ids)) != len(ids):
                raise ValueError("Candidate IDs must be unique")
            valid = []
            fingerprints: set[str] = set()
            for candidate in result.candidates:
                self._validate_candidate(candidate, catalog)
                candidate.tool_version = catalog[candidate.tool].version
                if candidate.fingerprint in fingerprints or not can_attempt(state, candidate, catalog[candidate.tool]):
                    continue
                fingerprints.add(candidate.fingerprint)
                valid.append(candidate)
            groups = []
            group_ids: set[str] = set()
            for group in result.groups:
                if group.id in group_ids or len(group.children) > state.budget.max_parallel_tools:
                    raise ValueError("Duplicate group ID or parallel limit exceeded")
                group_ids.add(group.id)
                seen: set[str] = set()
                for child in group.children:
                    self._validate_candidate(child, catalog)
                    child.tool_version = catalog[child.tool].version
                    if (child.tool == READ_EVIDENCE.name or not catalog[child.tool].retry_safe
                            or catalog[child.tool].approval_required or child.fingerprint in seen
                            or not can_attempt(state, child, catalog[child.tool])):
                        raise ValueError("Parallel children must be distinct, locally attested safe queries")
                    seen.add(child.fingerprint)
                if seen & fingerprints:
                    raise ValueError("Parallel queries duplicate a standalone candidate")
                fingerprints.update(seen)
                groups.append(group)
        except Exception as exc:
            # Keep any existing valid plan; malformed replacement output grants no new actions.
            state.no_progress += 1
            state.history.append(ActionRecord(choice="reason", status=f"invalid:{type(exc).__name__}",
                                              feedback=str(exc)[:2000]))
            self._save(state, "reasoning_rejected", error_type=type(exc).__name__)
            return
        state.memory = result.memory
        state.candidates = valid
        state.groups = groups
        state.no_progress = 0 if fingerprints - previous else state.no_progress + 1
        state.history.append(ActionRecord(choice="reason", status="success"))
        self._save(state, "reasoning", candidates=len(valid), stalled_plans=state.no_progress)

    def _usage(self, state: DecisionState, provider: Any, kind: str) -> None:
        usage = getattr(provider, "usage", None)
        if not isinstance(usage, list):
            return
        offset = self._usage_offsets.get(id(provider), 0)
        for entry in usage[offset:]:
            if not isinstance(entry, dict):
                continue
            inputs = entry.get("input_tokens", entry.get("prompt_tokens", 0))
            outputs = entry.get("output_tokens", entry.get("completion_tokens", 0))
            if all(type(n) is int and n >= 0 for n in (inputs, outputs)):
                state.model_tokens += inputs + outputs
                state.model_usage.append({"kind": kind, "input_tokens": inputs, "output_tokens": outputs})
        self._usage_offsets[id(provider)] = len(usage)

    def _execute_group(self, state: DecisionState, group: ActionGroup, confidence: float, deadline: float) -> None:
        """Reserve budgets/intents serially, run I/O concurrently, commit serially."""
        catalog = self._refresh(state)
        if (len(group.children) > state.budget.max_parallel_tools
                or state.tool_calls + len(group.children) > state.budget.max_tool_calls):
            raise ValueError("Parallel budget exhausted")
        records = []
        for child in group.children:
            self._validate_candidate(child, catalog)
            spec = catalog[child.tool]
            if not spec.retry_safe or spec.approval_required or not can_attempt(state, child, spec):
                raise ValueError("Parallel candidate is no longer eligible")
        for child in group.children:
            state.tool_calls += 1
            action = ActionRecord(choice=f"call:{child.id}", confidence=confidence, direction=child.direction,
                                  fingerprint=child.fingerprint, status="started", attempt_id=str(uuid4()),
                                  attempt_no=1 + sum(h.fingerprint == child.fingerprint for h in state.history))
            state.history.append(action)
            records.append((child, action, catalog[child.tool]))
        self._save(state, "parallel_started", group_id=group.id,
                   attempt_ids=[action.attempt_id for _, action, _ in records])

        def invoke(child: Candidate, spec: ToolSpec) -> tuple[ToolOutput, float]:
            started = self.clock()
            try:
                if started >= deadline:
                    raise DeadlineExceeded("Parallel query exceeded deadline")
                output = self.tools.execute(child)
                if output.status == "success" and spec.output_schema is not None:
                    validator(spec.output_schema).validate(output.data)
            except Exception as exc:
                output = tool_error_output(exc)
            return output, max(0, self.clock() - started)

        with ThreadPoolExecutor(max_workers=len(records), thread_name_prefix="tracepilot-query") as executor:
            futures = {executor.submit(copy_context().run, invoke, child, spec): (child, action, spec)
                       for child, action, spec in records}
            for future in as_completed(futures):
                child, action, spec = futures[future]
                output, elapsed = future.result()
                evidence = Evidence(task_id=state.task_id, tool=child.tool, source=spec.source,
                                    arguments=child.arguments, output=output, elapsed_seconds=elapsed,
                                    attempt_id=action.attempt_id, attempt_no=action.attempt_no)
                action.status, action.error_kind, action.evidence_id = output.status, output.error_kind, evidence.id
                state.evidence.append(evidence.summary())
                if output.status in {"success", "no_data"}:
                    state.no_progress = 0
                self._save(state, "evidence", evidence=evidence, evidence_id=evidence.id,
                           group_id=group.id, attempt_id=action.attempt_id, status=output.status)
        state.groups = [g for g in state.groups if g.id != group.id]
        self._save(state, "parallel_completed", group_id=group.id)
        if any(action.status == "approval_required" for _, action, _ in records):
            self._stop(state, "needs_input", "tool_requires_approval")

    def _stop(self, state: DecisionState, status: TerminalStatus, reason: str) -> None:
        state.pending_status = status
        state.stop_reason = reason
        self._save(state, "stop_requested", requested_status=status, reason=reason)

    def _attempt(
        self, state: DecisionState, candidate: Candidate, confidence: float, deadline: float,
    ) -> ToolOutput | None:
        # Revalidate every attempt, including retries after backoff.
        if self.clock() >= deadline:
            self._stop(state, "stopped", "time_budget")
            return None
        try:
            current = self._refresh(state)
            self._validate_candidate(candidate, current)
            if current[candidate.tool].version != candidate.tool_version:
                raise ValueError("Tool schema or retry policy changed during the decision")
        except Exception as exc:
            state.candidates = [item for item in state.candidates if item.tool != candidate.tool]
            state.history.append(ActionRecord(choice=f"call:{candidate.id}", status="stale_tool"))
            self._save(state, "tool_invalidated", tool=candidate.tool, error_type=type(exc).__name__)
            return None
        if not can_attempt(state, candidate, current[candidate.tool]):
            self._save(state, "call_blocked", tool=candidate.tool)
            return None
        if self.clock() >= deadline:
            self._stop(state, "stopped", "time_budget")
            return None

        old_direction = state.direction
        state.direction = candidate.direction
        state.tool_calls += 1
        action = ActionRecord(
            choice=f"call:{candidate.id}", confidence=confidence, direction=candidate.direction,
            fingerprint=candidate.fingerprint, status="started", attempt_id=str(uuid4()),
            attempt_no=1 + sum(item.fingerprint == candidate.fingerprint for item in state.history),
        )
        state.history.append(action)
        # Mandatory intent checkpoint before I/O; database errors never enter a tool retry.
        self._save(state, "tool_started", tool=candidate.tool, direction=candidate.direction,
                   previous_direction=old_direction, attempt_id=action.attempt_id, attempt_no=action.attempt_no)
        started = self.clock()
        try:
            if candidate.tool == READ_EVIDENCE.name:
                output = ToolOutput(status="success", data=self.store.read_evidence(state.task_id, **candidate.arguments))
            elif current[candidate.tool].approval_required:
                output = ToolOutput(status="approval_required", error="Operator approval is required")
            else:
                output = self.tools.execute(candidate)
                output_schema = current[candidate.tool].output_schema
                if output.status == "success" and output_schema is not None:
                    try:
                        validator(output_schema).validate(output.data)
                    except Exception:
                        output = ToolOutput(status="error", error="Tool result failed its output schema", error_kind="unknown")
        except Exception as exc:
            output = tool_error_output(exc)
        evidence = Evidence(
            task_id=state.task_id, tool=candidate.tool, source=current[candidate.tool].source,
            arguments=candidate.arguments, output=output, elapsed_seconds=max(0, self.clock() - started),
            attempt_id=action.attempt_id, attempt_no=action.attempt_no,
        )
        preview_limit = 30000 if candidate.tool == READ_EVIDENCE.name else 3000
        state.evidence.append(evidence.summary(max_chars=preview_limit))
        action.status = output.status
        action.error_kind = output.error_kind
        action.evidence_id = evidence.id
        # A completed query is coverage even when it found no matches. Different
        # sources returning the same payload must not erase each other's progress.
        if output.status in {"success", "no_data"}:
            state.no_progress = 0
        self._save(state, "evidence", evidence=evidence, evidence_id=evidence.id,
                   tool=candidate.tool, status=output.status, attempt_id=action.attempt_id,
                   attempt_no=action.attempt_no, error_kind=output.error_kind)
        if output.status == "approval_required":
            self._stop(state, "needs_input", "tool_requires_approval")
        return output

    def _execute(self, state: DecisionState, candidate: Candidate, confidence: float, deadline: float) -> None:
        spec = next(tool for tool in state.available_tools if tool.name == candidate.tool)
        remaining = state.budget.max_tool_attempts - sum(
            item.fingerprint == candidate.fingerprint for item in state.history
        )

        def retryable(output: ToolOutput | None) -> bool:
            return (output is not None and output.status == "error" and spec.retry_safe
                    and output.error_kind in TRANSIENT_TOOL_ERRORS and state.pending_status is None)

        def wait(retry_state: RetryCallState) -> float:
            assert retry_state.outcome is not None
            output = retry_state.outcome.result()
            return max(self.retry_wait(retry_state), output.retry_after_seconds or 0)

        def stop(retry_state: RetryCallState) -> bool:
            return (retry_state.attempt_number >= remaining
                    or state.tool_calls >= state.budget.max_tool_calls
                    or self.clock() + retry_state.upcoming_sleep >= deadline)

        def before_sleep(retry_state: RetryCallState) -> None:
            self._save(state, "tool_retry_scheduled", tool=candidate.tool,
                       fingerprint=candidate.fingerprint, wait_seconds=retry_state.upcoming_sleep)

        def exhausted(retry_state: RetryCallState) -> ToolOutput | None:
            assert retry_state.outcome is not None
            return retry_state.outcome.result()

        retryer = Retrying(
            retry=retry_if_result(retryable), wait=wait, stop=stop, sleep=self.sleep,
            before_sleep=before_sleep, retry_error_callback=exhausted,
        )
        output = retryer(self._attempt, state, candidate, confidence, deadline)
        if retryable(output):
            # Do not let a new candidate ID reset attempts or bypass Retry-After
            # when another retry could not fit inside the remaining budget.
            last = next(item for item in reversed(state.history) if item.fingerprint == candidate.fingerprint)
            last.retry_exhausted = True
            self._save(state, "tool_retry_exhausted", tool=candidate.tool, fingerprint=candidate.fingerprint)
        state.candidates = [item for item in state.candidates if item.fingerprint != candidate.fingerprint]

    def start(self, state: DecisionState) -> None:
        if (state.status != "running" or state.revision or state.history or state.evidence
                or state.candidates or state.groups or state.reasoning_calls or state.tool_calls or state.pending_status):
            raise ValueError("run requires a new task; existing tasks are not replayed by run; use resume explicitly")
        state.started_at = utc_now()
        state.deadline_at = state.started_at + timedelta(seconds=state.budget.max_seconds)
        self.store.create(state)

    def resume(self, state: DecisionState) -> None:
        """Reconcile unknown I/O without inventing an observation or resetting budgets."""
        if state.status != "running":
            raise ValueError("Only an owned running task can resume")
        if state.deadline_at is not None and state.deadline_at <= utc_now():
            self._stop(state, "stopped", "time_budget")
            return
        unknown = [h for h in state.history if h.status == "started" and h.attempt_id]
        if not unknown:
            return
        catalog = self._refresh(state)
        candidates = [*state.candidates, *(child for group in state.groups for child in group.children)]
        for action in unknown:
            candidate = next((c for c in candidates if c.fingerprint == action.fingerprint), None)
            spec = catalog.get(candidate.tool) if candidate else None
            action.status = "error" if spec and spec.retry_safe else "unknown"
            action.error_kind = "timeout" if spec and spec.retry_safe else "unknown"
            action.feedback = "Worker interrupted after intent; external result is unknown."
            if not spec or not spec.retry_safe:
                state.pending_status = "needs_input"
                state.stop_reason = "unknown_external_result"
        remaining_groups = []
        for group in state.groups:
            children = [c for c in group.children if c.tool in catalog and can_attempt(state, c, catalog[c.tool])]
            if len(children) == 1:
                state.candidates.extend(children)
            elif len(children) >= 2:
                remaining_groups.append(group.model_copy(update={"children": children}))
        state.groups = remaining_groups
        self._save(state, "attempt_results_unknown", attempt_ids=[h.attempt_id for h in unknown])

    def advance(self, state: DecisionState, *, deadline: float | None = None) -> None:
        """Execute one bounded decision; the caller owns the task lease."""
        if state.status != "running":
            raise ValueError("advance requires an owned running task")
        if state.pending_status is not None:
            return
        if state.started_at is None:
            state.started_at = utc_now()
            state.deadline_at = state.started_at + timedelta(seconds=state.budget.max_seconds)
            self._save(state, "investigation_started")
        if deadline is None:
            remaining = (state.deadline_at - utc_now()).total_seconds() if state.deadline_at else state.budget.max_seconds
            deadline = self.clock() + remaining
        with request_deadline(deadline, self.clock):
            if self.clock() >= deadline:
                self._stop(state, "stopped", "time_budget")
                return
            if state.steps >= state.budget.max_steps:
                self._stop(state, "stopped", "step_budget")
                return
            if state.budget.max_model_tokens is not None and state.model_tokens >= state.budget.max_model_tokens:
                self._stop(state, "stopped", "model_token_budget")
                return
            state.steps += 1
            self._save(state, "step_started")
            try:
                catalog = self._refresh(state)
            except Exception as exc:
                self._stop(state, "stopped" if self.clock() >= deadline else "failed",
                           "time_budget" if self.clock() >= deadline else f"tool_catalog_error:{type(exc).__name__}")
                return
            state.candidates = [c for c in state.candidates
                                if c.tool in catalog and can_attempt(state, c, catalog[c.tool])]
            state.groups = [g for g in state.groups if all(
                c.tool in catalog and can_attempt(state, c, catalog[c.tool]) for c in g.children)]
            if not state.reasoning_calls or state.context.get("replan_required"):
                if not can_reason(state):
                    self._stop(state, "needs_input", "new_context_without_reasoning_budget")
                    return
                self._reason(state, catalog)
                state.context.pop("replan_required", None)
            if state.budget.max_model_tokens is not None and state.model_tokens >= state.budget.max_model_tokens:
                self._stop(state, "stopped", "model_token_budget")
                return
            if self.clock() >= deadline:
                self._stop(state, "stopped", "time_budget")
                return
            actions = available_actions(state)
            try:
                try:
                    decision = self.decision_provider.decide(state.model_copy(deep=True), copy.deepcopy(actions))
                finally:
                    self._usage(state, self.decision_provider, "decision")
                if decision.choice not in actions:
                    raise ValueError("Decision selected an unavailable action")
            except Exception as exc:
                self._stop(state, "stopped" if self.clock() >= deadline else "failed",
                           "time_budget" if self.clock() >= deadline else f"decision_error:{type(exc).__name__}")
                return
            self._save(state, "decision", allowed_actions=list(actions), **decision.model_dump())
            if state.budget.max_model_tokens is not None and state.model_tokens >= state.budget.max_model_tokens:
                self._stop(state, "stopped", "model_token_budget")
                return
            if self.clock() >= deadline:
                self._stop(state, "stopped", "time_budget")
                return
            if decision.confidence < state.budget.min_confidence:
                state.history.append(ActionRecord(choice=decision.choice, confidence=decision.confidence,
                                                  status="low_confidence"))
                self._save(state, "low_confidence")
                if can_reason(state):
                    self._reason(state, catalog)
                else:
                    self._stop(state, "stopped", "low_confidence")
            elif decision.choice == "reason":
                self._reason(state, catalog)
            elif decision.choice == "need_input":
                self._stop(state, "needs_input", "missing_information")
            elif decision.choice == "finish":
                self._stop(state, "completed", "decision_finish")
            elif decision.choice.startswith("parallel:"):
                group = next(g for g in state.groups if f"parallel:{g.id}" == decision.choice)
                self._execute_group(state, group, decision.confidence, deadline)
            else:
                candidate = next(item for item in state.candidates if f"call:{item.id}" == decision.choice)
                self._execute(state, candidate, decision.confidence, deadline)
            self._save(state, "step_completed")

    def finalize(self, state: DecisionState, *, deadline: float | None = None) -> InvestigationResult:
        if state.pending_status is None:
            raise ValueError("Task has not requested a stop")
        if deadline is None:
            remaining = (state.deadline_at - utc_now()).total_seconds() if state.deadline_at else state.budget.max_seconds
            deadline = self.clock() + remaining
        try:
            if not self.store.verify(state.task_id)["valid"]:
                raise ValueError("Persisted evidence failed integrity checks")
            if self.clock() >= deadline or (
                    state.budget.max_model_tokens is not None and state.model_tokens >= state.budget.max_model_tokens):
                diagnosis = Diagnosis(
                    summary="调查已停止；当前结果仍需核验。" if any("\u4e00" <= c <= "\u9fff" for c in state.question)
                    else "Investigation stopped; evidence still requires verification.",
                    limitations=[state.stop_reason, "Report model call omitted because the task budget is exhausted."],
                    schema_version=2, outcome="insufficient_evidence",
                )
            else:
                with request_deadline(deadline, self.clock):
                    try:
                        draft = DiagnosisDraft.model_validate(self.reasoner.report(state.model_copy(deep=True)))
                    finally:
                        self._usage(state, getattr(self.reasoner, "llm", None), "report")
                    if self.clock() >= deadline:
                        raise DeadlineExceeded("Report exceeded the investigation deadline")
                    diagnosis = render_diagnosis(draft, state, self._observation_ids(state))
                    state.memory = draft.memory
        except DeadlineExceeded:
            state.pending_status = "stopped"
            state.stop_reason = "time_budget"
            diagnosis = Diagnosis(summary="Investigation deadline exhausted; no validated report is available.",
                                  limitations=["Report deadline exhausted"], schema_version=2, outcome="insufficient_evidence")
        except Exception as exc:
            original_stop = state.stop_reason
            state.pending_status = "failed"
            state.stop_reason = "report_validation_failed"
            diagnosis = Diagnosis(
                summary="Investigation ended; a validated diagnosis could not be generated.",
                limitations=[original_stop, f"Report rejected: {type(exc).__name__}"],
                verification_steps=["Inspect the persisted evidence and audit events before drawing conclusions."],
                schema_version=2, outcome="invalid_report",
            )
        state.status = state.pending_status
        state.pending_status = None
        self._save(state, "report", diagnosis=diagnosis)
        return InvestigationResult(state=state, diagnosis=diagnosis)

    def run(self, state: DecisionState) -> InvestigationResult:
        self.start(state)
        deadline = self.clock() + state.budget.max_seconds
        while state.pending_status is None:
            self.advance(state, deadline=deadline)
        return self.finalize(state, deadline=deadline)
