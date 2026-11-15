import json
import threading
from types import SimpleNamespace
from datetime import timedelta
from unittest.mock import Mock

import pytest
from sqlalchemy import update

from tracepilot.engine import DecisionAgent
from tracepilot.limits import DeadlineExceeded, request_deadline
from tracepilot.models import ActionGroup, Budget, Candidate, Claim, Decision, DecisionState, DiagnosisDraft, ReasoningResult, TaskMemory, ToolOutput
from tracepilot.providers import JevDecisionProvider, StructuredReasoner
from tracepilot.scenarios import ScenarioDecisionProvider, ScenarioReasoner, ScenarioTools, demo_scope
from tracepilot.store import EvidenceStore
from tracepilot.evaluation import LLMDecisionProvider, evaluate_fixture


@pytest.fixture
def store(tmp_path):
    value = EvidenceStore.from_url(f"sqlite:///{tmp_path / 'runtime.db'}")
    value.initialize()
    yield value
    value.engine.dispose()


def group_reasoner():
    result = Mock(wraps=ScenarioReasoner())
    args = demo_scope().model_dump(mode="json")
    result.reason.return_value = ReasoningResult(memory=TaskMemory(), groups=[ActionGroup(
        id="initial", purpose="Independent instance and trace observations", children=[
            Candidate(id=name, tool=name, arguments=args, purpose=name, direction="initial")
            for name in ["service_instances", "service_traces"]])])
    return result


def group_provider():
    result = Mock()
    result.decide.side_effect = lambda state, actions: Decision(
        choice=next((key for key in actions if key.startswith("parallel:")), "finish"), confidence=1)
    return result


def test_parallel_io_with_serial_checkpoints_and_reserved_budget(store):
    gateway = ScenarioTools("db-pool")
    barrier = threading.Barrier(2)
    execute = gateway.execute
    worker_threads = set()
    checkpoint_threads = set()
    checkpoint = store.checkpoint

    def query(candidate):
        worker_threads.add(threading.get_ident())
        barrier.wait(timeout=3)
        return execute(candidate)

    def save(state, **kwargs):
        checkpoint_threads.add(threading.get_ident())
        return checkpoint(state, **kwargs)

    gateway.execute, store.checkpoint = query, save
    result = DecisionAgent(group_reasoner(), group_provider(), gateway, store).run(DecisionState(
        question="Parallel initial observation", scope=demo_scope(), budget=Budget(max_parallel_tools=2, max_tool_calls=2)))
    assert result.state.status == "completed"
    assert len(worker_threads) == 2 and len(checkpoint_threads) == 1
    assert result.state.tool_calls == len(result.state.evidence) == 2
    assert store.verify(result.state.task_id)["valid"]
    assert len({e["attempt_id"] for e in result.state.evidence}) == 2


def test_parallel_cannot_overspend_last_tool_slot(store):
    gateway = Mock(wraps=ScenarioTools("db-pool"))
    provider = group_provider()
    result = DecisionAgent(group_reasoner(), provider, gateway, store).run(DecisionState(
        question="Bounded", scope=demo_scope(), budget=Budget(max_parallel_tools=2, max_tool_calls=1)))
    gateway.execute.assert_not_called()
    assert not any(key.startswith("parallel:") for key in provider.decide.call_args.args[1])
    assert result.state.status == "completed"


def test_parallel_preserves_success_when_sibling_fails(store):
    gateway = ScenarioTools("db-pool")
    execute = gateway.execute
    gateway.execute = lambda c: ToolOutput(status="error", error_kind="permission") if c.tool == "service_traces" else execute(c)
    result = DecisionAgent(group_reasoner(), group_provider(), gateway, store).run(DecisionState(
        question="Partial result", scope=demo_scope(), budget=Budget(max_parallel_tools=2)))
    assert sorted(e["status"] for e in result.state.evidence) == ["error", "success"]
    assert store.verify(result.state.task_id)["valid"]


def test_unknown_unsafe_result_stops_for_operator_without_tool_replay(store):
    gateway = Mock(wraps=ScenarioTools("db-pool"))
    gateway.catalog.return_value = [s.model_copy(update={"retry_safe": False}) for s in gateway.catalog()]
    state = DecisionState(question="Crash", scope=demo_scope())
    agent = DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), gateway, store)
    agent.start(state)
    agent.on_event = lambda e: (_ for _ in ()).throw(SystemExit()) if e["kind"] == "tool_started" else None
    with pytest.raises(SystemExit):
        agent.advance(state)
    saved = store.load(state.task_id)
    agent.on_event = None
    agent.resume(saved)
    assert saved.pending_status == "needs_input"
    gateway.execute.assert_not_called()
    assert saved.tool_calls == 1 and not saved.evidence


def test_resume_after_evidence_commit_does_not_duplicate_query(store):
    gateway = ScenarioTools("db-pool")
    agent = DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), gateway, store)
    state = DecisionState(question="Crash after commit", scope=demo_scope())
    agent.start(state)
    agent.on_event = lambda e: (_ for _ in ()).throw(SystemExit()) if e["kind"] == "evidence" else None
    with pytest.raises(SystemExit):
        agent.advance(state)
    saved = store.load(state.task_id)
    agent.on_event = None
    agent.resume(saved)
    while saved.pending_status is None:
        agent.advance(saved)
    result = agent.finalize(saved)
    assert len(gateway.calls) == result.state.tool_calls == 5
    assert sum(c.tool == "service_instances" for c in gateway.calls) == 1


def test_persisted_deadline_includes_report_without_reset_on_resume(store):
    reasoner = Mock(wraps=ScenarioReasoner())
    agent = DecisionAgent(reasoner, ScenarioDecisionProvider(), ScenarioTools("db-pool"), store)
    state = DecisionState(question="Deadline", scope=demo_scope())
    agent.start(state)
    state.deadline_at = state.started_at - timedelta(seconds=1)
    store.checkpoint(state, event={"kind": "expired"})
    saved = store.load(state.task_id)
    agent.advance(saved)
    result = agent.finalize(saved)
    reasoner.reason.assert_not_called()
    reasoner.report.assert_not_called()
    assert result.state.stop_reason == "time_budget" and result.state.status == "stopped"


def test_late_report_is_not_committed_as_success(store):
    now = [0.0]
    reasoner = Mock(wraps=ScenarioReasoner())
    original = reasoner.report

    def report(state):
        now[0] = 100
        return DiagnosisDraft(memory=TaskMemory())

    reasoner.report.side_effect = report
    result = DecisionAgent(reasoner, ScenarioDecisionProvider(), ScenarioTools("db-pool"), store,
                           clock=lambda: now[0]).run(DecisionState(question="Late report", scope=demo_scope(), budget=Budget(max_seconds=10)))
    assert result.state.status == "stopped" and result.state.stop_reason == "time_budget"


def test_invalid_remediation_citation_is_rejected(store):
    reasoner = Mock(wraps=ScenarioReasoner())
    reasoner.report.return_value = DiagnosisDraft(memory=TaskMemory(), remediation_suggestions=[{
        "description": "Increase pool", "evidence_ids": ["forged"], "risks": ["Database overload"]}])
    result = DecisionAgent(reasoner, ScenarioDecisionProvider(), ScenarioTools("db-pool"), store).run(DecisionState(question="Advice", scope=demo_scope()))
    assert result.diagnosis.outcome == "invalid_report"


def test_decision_view_preserves_recent_observation_without_full_raw_logs():
    state = DecisionState(question="Choose next", evidence=[{"id": "one", "status": "success", "preview": "x" * 10000}])
    assert len(state.decision_state()["evidence"][0]["preview"]) == 800
    assert len(state.model_state()["evidence"][0]["preview"]) == 10000


def test_deadline_prevents_jev_request():
    provider = JevDecisionProvider("test")
    provider._post = Mock(wraps=provider._post)
    with request_deadline(0, lambda: 1), pytest.raises(DeadlineExceeded):
        provider.decide(DecisionState(question="Expired"), {"finish": "Finish", "need_input": "Need input"})


def test_partial_parallel_checkpoint_recovers_only_uncommitted_children(store):
    gateway = ScenarioTools("db-pool")
    provider = Mock()
    provider.decide.side_effect = lambda state, actions: Decision(choice=next(
        (k for k in actions if k.startswith("parallel:")), next((k for k in actions if k.startswith("call:")), "finish")), confidence=1)
    agent = DecisionAgent(group_reasoner(), provider, gateway, store)
    state = DecisionState(question="Partial parallel checkpoint", scope=demo_scope(), budget=Budget(max_parallel_tools=2))
    agent.start(state)
    agent.on_event = lambda e: (_ for _ in ()).throw(SystemExit()) if e["kind"] == "evidence" else None
    with pytest.raises(SystemExit):
        agent.advance(state)
    saved = store.load(state.task_id)
    committed_tool = saved.evidence[0]["tool"]
    agent.on_event = None
    agent.resume(saved)
    while saved.pending_status is None:
        agent.advance(saved)
    agent.finalize(saved)
    assert sum(c.tool == committed_tool for c in gateway.calls) == 1
    assert saved.tool_calls == 3 and len(saved.evidence) == 2
    assert store.verify(saved.task_id)["valid"]


def test_known_token_budget_stops_before_another_model_call(store):
    reasoner = Mock(wraps=ScenarioReasoner())
    reasoner.llm = SimpleNamespace(usage=[])
    original = ScenarioReasoner()

    def reason(state):
        reasoner.llm.usage.append({"input_tokens": 100, "output_tokens": 20})
        return original.reason(state)

    reasoner.reason.side_effect = reason
    provider = Mock(wraps=ScenarioDecisionProvider())
    result = DecisionAgent(reasoner, provider, ScenarioTools("db-pool"), store).run(DecisionState(
        question="Token budget", scope=demo_scope(), budget=Budget(max_model_tokens=100)))
    provider.decide.assert_not_called()
    reasoner.report.assert_not_called()
    assert result.state.model_tokens == 120 and result.state.stop_reason == "model_token_budget"


def test_llm_comparison_provider_cannot_escape_runtime_actions():
    client = Mock()
    client.completion.return_value = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(
        content='{"choice":"delete_cluster","confidence":1}'))])
    with pytest.raises(ValueError, match="unavailable"):
        LLMDecisionProvider(client).decide(DecisionState(question="Compare"), {"finish": "Finish"})


def test_fixture_evaluation_does_not_claim_semantic_accuracy(store):
    result = evaluate_fixture("db-pool", ScenarioReasoner(), ScenarioDecisionProvider(), store)
    assert result["outcome_match"] and result["integrity_valid"]
    assert result["evidence_coverage"] == 1
    assert "not semantic" in result["limitations"]


def test_transient_catalog_timeout_can_recover_before_any_tool_execution(store):
    gateway = Mock(wraps=ScenarioTools("db-pool"))
    real = ScenarioTools("db-pool")
    first = [True]

    def catalog(refresh=True):
        if first[0]:
            first[0] = False
            raise TimeoutError("Transient discovery timeout")
        return real.catalog(refresh)

    gateway.catalog.side_effect = catalog
    result = DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), gateway, store).run(DecisionState(question="Recover", scope=demo_scope()))
    assert result.state.status == "completed" and result.state.tool_calls == 5
