import copy
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, update
from sqlalchemy.schema import CreateSchema, DropSchema

from tracepilot.api import create_app
from tracepilot.engine import DecisionAgent
from tracepilot.models import DecisionState, InvestigationScope
from tracepilot.oncall import OnCallConfig, Webhook
from tracepilot.scenarios import ScenarioDecisionProvider, ScenarioReasoner, ScenarioTools, demo_scope
from tracepilot.store import EvidenceStore
from tracepilot.task_store import LeasedEvidenceStore, LeaseLost, TaskConflict, TaskStore
from tracepilot.worker import Worker, build_agent


@pytest.fixture
def config(monkeypatch):
    monkeypatch.setenv("ONCALL_SOURCE", "source-token-at-least-sixteen")
    monkeypatch.setenv("ONCALL_API", "api-token-at-least-sixteen")
    monkeypatch.setenv("OTHER_API", "other-api-token-at-least-sixteen")
    return OnCallConfig.model_validate({"lease_seconds": 3, "sources": {"prod": {
        "tenant_id": "one", "source_group": "am", "cluster": "prod", "token_env": "ONCALL_SOURCE",
        "namespaces": ["demo"], "services": ["checkout", "inventory"], "demo_scenario": "db-pool",
        "dependencies": {"checkout": ["inventory"]},
    }}, "principals": {"one": {"tenant_id": "one", "token_env": "ONCALL_API"},
                        "other": {"tenant_id": "other", "token_env": "OTHER_API"}}})


@pytest.fixture(params=["sqlite", "postgresql"])
def tasks(request, tmp_path, config):
    schema = None
    if request.param == "postgresql":
        url = os.environ.get("HOLMES_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("Set HOLMES_TEST_POSTGRES_URL for PostgreSQL queue integration")
        base = create_engine(url, pool_pre_ping=True)
        schema = "oncall_" + uuid4().hex
        with base.begin() as conn:
            conn.execute(CreateSchema(schema))
        engine = base.execution_options(schema_translate_map={None: schema})
    else:
        engine = create_engine(f"sqlite:///{tmp_path / 'oncall.db'}")
        base = engine
    result = TaskStore(EvidenceStore(engine), config)
    result.initialize()
    yield result
    if schema:
        with base.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))
    base.dispose()


def notification(status="firing", *, fingerprint="one", starts=None, name="HighLatency", pod="checkout-a"):
    start = starts or datetime.now(timezone.utc) - timedelta(minutes=1)
    alert = {"status": status, "labels": {"service": "checkout", "namespace": "demo", "alertname": name, "pod": pod},
             "annotations": {"summary": "Checkout timeout"}, "startsAt": start.isoformat(), "fingerprint": fingerprint}
    if status == "resolved":
        alert["endsAt"] = datetime.now(timezone.utc).isoformat()
    return {"status": status, "groupKey": "not-an-incident-id", "alerts": [alert]}


def ingest(tasks, data=None):
    return tasks.ingest("prod", Webhook.model_validate(data or notification()))


def test_webhook_duplicates_and_moving_firing_end_do_not_create_tasks(tasks):
    data = notification()
    first = ingest(tasks, data)
    data["alerts"][0]["endsAt"] = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    repeated = ingest(tasks, data)
    assert first["created_tasks"] == 1
    assert repeated["created_tasks"] == repeated["updated_alerts"] == 0
    assert first["task_ids"] == repeated["task_ids"]
    state = tasks.evidence.load(first["task_ids"][0])
    assert state.status == "queued" and state.tool_calls == 0
    assert state.allowed_services == ["checkout", "inventory"]


def test_resolved_before_firing_does_not_resurrect_and_new_start_is_new_episode(tasks):
    data = notification("resolved")
    assert ingest(tasks, data)["created_tasks"] == 0
    data["alerts"][0]["status"] = "firing"
    assert ingest(tasks, data)["created_tasks"] == 0
    data["alerts"][0]["startsAt"] = datetime.now(timezone.utc).isoformat()
    assert ingest(tasks, data)["created_tasks"] == 1


def test_group_status_and_group_key_do_not_override_each_alert(tasks):
    data = notification()
    data["status"] = "resolved"
    data["alerts"] += notification("resolved", fingerprint="two")["alerts"]
    data["alerts"] += notification(fingerprint="three", name="OtherFailure")["alerts"]
    result = ingest(tasks, data)
    assert result["created_tasks"] == 2
    assert len(result["task_ids"]) == 2


def test_same_rule_resource_can_correlate_but_same_service_alone_does_not(tasks):
    data = notification()
    data["alerts"] += notification(fingerprint="two", starts=datetime.now(timezone.utc) - timedelta(seconds=30))["alerts"]
    assert ingest(tasks, data)["created_tasks"] == 1
    assert ingest(tasks, notification(fingerprint="three", pod="checkout-b"))["created_tasks"] == 1


def test_recovery_notification_updates_incident_without_finishing_task(tasks):
    data = notification()
    task_id = ingest(tasks, data)["task_ids"][0]
    data["alerts"][0].update(status="resolved", endsAt=datetime.now(timezone.utc).isoformat())
    ingest(tasks, data)
    assert tasks.evidence.load(task_id).status == "queued"
    job = tasks.job(task_id, "one")
    assert tasks.incident(job["incident_id"], "one")["status"] == "resolved"


def test_scope_rejection_is_atomic_for_whole_batch(tasks):
    data = notification()
    data["alerts"] += notification(fingerprint="two")["alerts"]
    data["alerts"][1]["labels"]["namespace"] = "private"
    with pytest.raises(PermissionError):
        ingest(tasks, data)
    with tasks.engine.connect() as conn:
        assert conn.execute(select(tasks.jobs)).first() is None
        assert conn.execute(select(tasks.receipts)).first() is None


def test_tasks_events_and_queue_rollback_together(tasks, monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError("database interrupted")
    monkeypatch.setattr(tasks.evidence, "checkpoint", unavailable)
    with pytest.raises(OSError):
        ingest(tasks)
    with tasks.engine.connect() as conn:
        assert conn.execute(select(tasks.jobs)).first() is None
        assert conn.execute(select(tasks.evidence.tasks)).first() is None
        assert conn.execute(select(tasks.alerts)).first() is None


def test_lease_reclaim_fences_old_worker_even_with_latest_checkpoint(tasks):
    task_id = ingest(tasks)["task_ids"][0]
    first = tasks.claim("worker-a")
    assert tasks.claim("worker-b") is None
    with tasks.engine.begin() as conn:
        conn.execute(update(tasks.jobs).where(tasks.jobs.c.id == task_id).values(lease_until=0))
    second = tasks.claim("worker-b")
    assert second["lease_epoch"] == first["lease_epoch"] + 1
    stale = LeasedEvidenceStore(tasks, first)
    with pytest.raises(LeaseLost):
        stale.checkpoint(stale.load(task_id), event={"kind": "stale-result"})
    assert not tasks.heartbeat(first)
    assert tasks.heartbeat(second)


def test_cancel_fences_worker_and_preserves_history(tasks):
    task_id = ingest(tasks)["task_ids"][0]
    token = tasks.claim("worker")
    tasks.control(task_id, "one", cancel=True)
    assert tasks.evidence.load(task_id).status == "cancelled"
    leased = LeasedEvidenceStore(tasks, token)
    with pytest.raises(LeaseLost):
        leased.checkpoint(leased.load(task_id), event={"kind": "late"})
    assert tasks.claim("other") is None


def test_worker_drives_alert_to_cited_report_and_reuses_completed_queries(tasks):
    data = notification()
    task_id = ingest(tasks, data)["task_ids"][0]
    assert Worker(tasks).run_once()
    state = tasks.evidence.load(task_id)
    assert state.status == "completed" and state.tool_calls == 5
    assert tasks.evidence.load_diagnosis(task_id).outcome == "candidate_causes"
    assert tasks.evidence.verify(task_id)["valid"]
    assert ingest(tasks, data)["created_tasks"] == 0
    assert not Worker(tasks).run_once()


def test_unknown_read_result_can_retry_but_attempt_budget_survives_crash(tasks):
    task_id = tasks.submit("prod", demo_scope(), "Investigate")
    token = tasks.claim("dead-worker")
    store = LeasedEvidenceStore(tasks, token)
    agent = DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), ScenarioTools("db-pool"), store)
    state = store.load(task_id)

    def crash(event):
        if event["kind"] == "tool_started":
            raise SystemExit("simulated process death")

    agent.on_event = crash
    with pytest.raises(SystemExit):
        agent.advance(state)
    checkpoint = store.load(task_id)
    assert checkpoint.tool_calls == 1 and checkpoint.history[-1].status == "started"
    with tasks.engine.begin() as conn:
        conn.execute(update(tasks.jobs).where(tasks.jobs.c.id == task_id).values(lease_until=0))
    Worker(tasks).run_once()
    final = tasks.evidence.load(task_id)
    assert final.status == "completed" and final.tool_calls == 6
    assert final.history[1].feedback.startswith("Worker interrupted")
    assert len(final.evidence) == 5  # Unknown result is not manufactured as evidence.


def test_completed_report_before_worker_crash_is_not_replayed(tasks):
    task_id = tasks.submit("prod", demo_scope(), "Investigate")
    token = tasks.claim("dead-worker")
    store = LeasedEvidenceStore(tasks, token)
    agent = DecisionAgent(ScenarioReasoner(), ScenarioDecisionProvider(), ScenarioTools("db-pool"), store)
    state = store.load(task_id)
    while state.pending_status is None:
        agent.advance(state)
    agent.finalize(state)
    with tasks.engine.begin() as conn:
        conn.execute(update(tasks.jobs).where(tasks.jobs.c.id == task_id).values(lease_until=0))
    assert tasks.claim("replacement") is None
    assert tasks.job(task_id, "one")["queue_state"] == "terminal"
    assert tasks.evidence.load(task_id).tool_calls == 5


def test_new_alert_during_report_schedules_one_followup(tasks):
    data = notification()
    task_id = ingest(tasks, data)["task_ids"][0]

    class UpdatingReasoner(ScenarioReasoner):
        def report(self, state):
            updated = copy.deepcopy(data)
            updated["alerts"][0]["annotations"]["summary"] = "New important context"
            ingest(tasks, updated)
            return super().report(state)

    def factory(state, store, source):
        return DecisionAgent(UpdatingReasoner(), ScenarioDecisionProvider(), ScenarioTools("db-pool"), store)

    Worker(tasks, factory).run_once()
    incident = tasks.incident(tasks.job(task_id, "one")["incident_id"], "one")
    assert len(incident["task_ids"]) == 2
    assert incident["active_task_id"] != task_id


def test_api_auth_isolation_body_limit_and_sse_cursor(tasks, config):
    source_headers = {"Authorization": "Bearer source-token-at-least-sixteen"}
    api_headers = {"Authorization": "Bearer api-token-at-least-sixteen"}
    other_headers = {"Authorization": "Bearer other-api-token-at-least-sixteen"}
    with TestClient(create_app(config, tasks)) as client:
        assert client.post("/api/v1/webhooks/alertmanager/prod", json=notification()).status_code == 401
        response = client.post("/api/v1/webhooks/alertmanager/prod", json=notification(), headers=source_headers)
        assert response.status_code == 202
        task_id = response.json()["task_ids"][0]
        assert client.get(f"/api/v1/tasks/{task_id}", headers=source_headers).status_code == 401
        assert client.get(f"/api/v1/tasks/{task_id}", headers=other_headers).status_code == 404
        assert client.get(f"/api/v1/tasks/{task_id}", headers=api_headers).json()["state"]["status"] == "queued"
        Worker(tasks).run_once()
        result = client.get(f"/api/v1/tasks/{task_id}", headers=api_headers).json()
        evidence_id = result["state"]["evidence"][0]["id"]
        assert client.get(f"/api/v1/tasks/{task_id}/evidence/{evidence_id}", headers=other_headers).status_code == 404
        assert client.get(f"/api/v1/tasks/{task_id}/evidence/{evidence_id}?limit=30", headers=api_headers).status_code == 200
        stream = client.get(f"/api/v1/tasks/{task_id}/events?follow=false", headers=api_headers)
        assert "event: report" in stream.text
        ids = [int(line[4:]) for line in stream.text.splitlines() if line.startswith("id: ")]
        assert ids == sorted(set(ids))
        tail = client.get(f"/api/v1/tasks/{task_id}/events?follow=false", headers={**api_headers, "Last-Event-ID": str(ids[-2])})
        assert [int(line[4:]) for line in tail.text.splitlines() if line.startswith("id: ")] == [ids[-1]]
        assert client.get(f"/api/v1/tasks/{task_id}/events", headers={**api_headers, "Last-Event-ID": "bad"}).status_code == 400
        assert client.post("/api/v1/webhooks/alertmanager/prod", content=b"x" * (config.max_body_bytes + 1), headers=source_headers).status_code == 413


def test_manual_api_and_resume_require_scope_and_waiting_status(tasks, config):
    headers = {"Authorization": "Bearer api-token-at-least-sixteen"}
    with TestClient(create_app(config, tasks)) as client:
        data = {"source_id": "prod", "scope": demo_scope().model_dump(mode="json"), "question": "Investigate"}
        response = client.post("/api/v1/investigations", json=data, headers=headers)
        assert response.status_code == 202
        task_id = response.json()["task_id"]
        assert client.post(f"/api/v1/tasks/{task_id}/resume", json={"input_text": "Continue"}, headers=headers).status_code == 409
        data["scope"]["namespace"] = "private"
        assert client.post("/api/v1/investigations", json=data, headers=headers).status_code == 403
        assert client.post(f"/api/v1/tasks/{task_id}/cancel", headers=headers).status_code == 202


def test_waiting_resume_keeps_deadline_and_counters(tasks):
    task_id = tasks.submit("prod", demo_scope(), "Investigate")
    token = tasks.claim("worker")
    state = tasks.evidence.load(task_id)
    state.started_at = datetime.now(timezone.utc)
    state.deadline_at = state.started_at + timedelta(seconds=300)
    state.status, state.steps, state.tool_calls = "needs_input", 4, 2
    tasks.evidence.checkpoint(state, event={"kind": "needs_input"})
    tasks.finish(token, state)
    tasks.control(task_id, "one", input_text="Authorized context, not a tool instruction")
    resumed = tasks.evidence.load(task_id)
    assert (resumed.steps, resumed.tool_calls, resumed.deadline_at) == (4, 2, state.deadline_at)
    assert resumed.status == "queued"
    assert tasks.claim("another") is not None


def test_postgresql_concurrent_ingress_and_claim_are_idempotent(tasks):
    if tasks.engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL row-lock and SKIP LOCKED integration")
    data = notification()
    barrier = threading.Barrier(2)

    def receive(_):
        barrier.wait(timeout=5)
        return ingest(tasks, data)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(receive, range(2)))
    assert sum(r["created_tasks"] for r in results) == 1
    task_id = results[0]["task_ids"][0]
    assert results[1]["task_ids"] == [task_id]
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda owner: tasks.claim(owner), ["one", "two"]))
    assert sum(token is not None for token in claims) == 1


def test_postgresql_concurrent_alerts_share_one_active_investigation(tasks):
    if tasks.engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL concurrent incident correlation")
    barrier = threading.Barrier(2)
    starts = datetime.now(timezone.utc) - timedelta(seconds=30)

    def receive(number):
        data = notification(fingerprint=f"alert-{number}", starts=starts)
        barrier.wait(timeout=5)
        return ingest(tasks, data)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(receive, range(2)))
    assert sum(r["created_tasks"] for r in results) == 1
    assert results[0]["task_ids"] == results[1]["task_ids"]


def test_truncated_recovery_is_unknown_and_idle_incidents_do_not_merge_forever(tasks):
    data = notification()
    task_id = ingest(tasks, data)["task_ids"][0]
    incident_id = tasks.job(task_id, "one")["incident_id"]
    data["alerts"][0].update(status="resolved", endsAt=datetime.now(timezone.utc).isoformat())
    data["truncatedAlerts"] = 1
    ingest(tasks, data)
    assert tasks.incident(incident_id, "one")["status"] == "unknown"
    with tasks.engine.begin() as conn:
        conn.execute(update(tasks.incidents).where(tasks.incidents.c.id == incident_id).values(updated_at=0))
    other = ingest(tasks, notification(fingerprint="new-episode"))["task_ids"][0]
    assert tasks.job(other, "one")["incident_id"] != incident_id


def test_expired_input_wait_stops_without_resetting_budget(tasks):
    task_id = tasks.submit("prod", demo_scope(), "Wait")
    token = tasks.claim("worker")
    state = tasks.evidence.load(task_id)
    state.status, state.steps = "needs_input", 4
    tasks.evidence.checkpoint(state, event={"kind": "needs_input"})
    tasks.finish(token, state)
    with tasks.engine.begin() as conn:
        conn.execute(update(tasks.jobs).where(tasks.jobs.c.id == task_id).values(wait_until=0))
    assert tasks.expire_waiting() == 1
    saved = tasks.evidence.load(task_id)
    assert saved.status == "stopped" and saved.stop_reason == "input_wait_timeout" and saved.steps == 4


def test_worker_rechecks_cluster_and_service_authority(tasks):
    task_id = tasks.submit("prod", demo_scope(), "Authority")
    token = tasks.claim("worker")
    store = LeasedEvidenceStore(tasks, token)
    state = store.load(task_id)
    changed_source = tasks.config.sources["prod"].model_copy(update={"cluster": "different-cluster"})
    with pytest.raises(PermissionError):
        build_agent(state, store, changed_source)


def test_metrics_are_tenant_scoped(tasks, config):
    ingest(tasks)
    assert tasks.metrics("one")["tasks_by_queue_state"] == {"ready": 1}
    assert tasks.metrics("other")["accepted_webhooks"] == 0


def test_invalid_config_cannot_alias_other_tenant_secret(config, monkeypatch):
    monkeypatch.setenv("OTHER_API", "api-token-at-least-sixteen")
    with pytest.raises(ValueError, match="different tenants"):
        config.validate_secrets()
