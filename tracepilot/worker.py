"""Durable queue worker. Run separately from the API, sharing PostgreSQL."""

import logging
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator
from uuid import uuid4
from sqlalchemy.exc import SQLAlchemyError

from tracepilot.config import AgentConfig
from tracepilot.engine import DecisionAgent
from tracepilot.llm import HTTPCompletion
from tracepilot.mcp_gateway import MCPGateway, required_environment
from tracepilot.models import DecisionState, Diagnosis
from tracepilot.oncall import Source
from tracepilot.providers import JevDecisionProvider, StructuredReasoner
from tracepilot.scenarios import ScenarioDecisionProvider, ScenarioReasoner, ScenarioTools
from tracepilot.task_store import LeasedEvidenceStore, LeaseLost, TaskStore
from tracepilot.store import StaleCheckpoint


log = logging.getLogger(__name__)


def build_agent(state: DecisionState, store: LeasedEvidenceStore, source: Source) -> DecisionAgent:
    if (state.scope is None or state.scope.service not in source.services or state.scope.namespace not in source.namespaces
            or state.context.get("tenant_id") != source.tenant_id or state.context.get("cluster") != source.cluster):
        raise PermissionError("Persisted task no longer matches the source authority")
    state.allowed_services = [service for service in state.allowed_services
                              if service in source.authorized_services(state.scope.service)]
    if state.scope.service not in state.allowed_services:
        raise PermissionError("Primary service permission has been revoked")
    if source.demo_scenario is not None:
        return DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), ScenarioTools(source.demo_scenario), store)
    if source.agent_config is None:
        raise ValueError("Live workers require agent configuration and a scope")
    config = AgentConfig.load(source.agent_config)
    return DecisionAgent(StructuredReasoner(HTTPCompletion(config.llm)),
        JevDecisionProvider(required_environment(config.jev_api_key_env), config.jev_model),
        MCPGateway(config.mcp_servers, state.scope, state.allowed_services), store)


class Worker:
    def __init__(self, tasks: TaskStore, factory: Callable[..., DecisionAgent] = build_agent, owner: str | None = None):
        self.tasks, self.factory, self.owner = tasks, factory, owner or str(uuid4())

    @contextmanager
    def _keepalive(self, token: dict[str, Any]) -> Iterator[None]:
        stop = threading.Event()

        def beat() -> None:
            while not stop.wait(self.tasks.config.lease_seconds / 3):
                try:
                    if not self.tasks.heartbeat(token):
                        return
                except Exception:
                    # Writes and every subsequent step still perform a database fence.
                    log.warning("Task heartbeat failed; lease fencing remains active", extra={"task_id": token["id"]})
                    return

        thread = threading.Thread(target=beat, daemon=True, name="tracepilot-lease")
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=1)

    def run_once(self) -> bool:
        self.tasks.expire_waiting()
        token = self.tasks.claim(self.owner)
        if token is None:
            return False
        store = LeasedEvidenceStore(self.tasks, token)
        state = store.load(token["id"])
        agent = None
        with self._keepalive(token):
            try:
                agent = self.factory(state, store, self.tasks.config.sources[token["source_id"]])
                agent.resume(state)
                while state.pending_status is None:
                    self.tasks.sync_context(token, state)
                    agent.advance(state)
                agent.finalize(state)
                self.tasks.finish(token, state)
            except LeaseLost:
                return True
            except (SQLAlchemyError, StaleCheckpoint):
                log.warning("Checkpoint unavailable; task will recover after lease expiry", extra={"task_id": token["id"]})
                return True
            except Exception as exc:
                # Storage failures must not be converted into tool retries. If this
                # write also fails, the lease expires and a future worker reconciles.
                state.status, state.pending_status, state.stop_reason = "failed", None, f"worker_error:{type(exc).__name__}"
                diagnosis = Diagnosis(summary="Investigation failed; inspect retained evidence before drawing conclusions.",
                                      limitations=[state.stop_reason], schema_version=2, outcome="invalid_report")
                try:
                    store.checkpoint(state, event={"kind": "worker_failed"}, diagnosis=diagnosis)
                    self.tasks.finish(token, state)
                except LeaseLost:
                    pass
        return True

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        while not stop.is_set():
            try:
                busy = self.run_once()
            except Exception:
                log.exception("Task polling failed")
                busy = False
            if not busy:
                stop.wait(self.tasks.config.poll_seconds)
