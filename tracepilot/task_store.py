"""Additive PostgreSQL/SQLite durable queue sharing the existing evidence transaction.

Scheduling metadata lives beside the original task row. DecisionState remains the
only runtime state. PostgreSQL uses row locks; conditional updates also fence
SQLite contenders. No in-memory queue is used for accepted alerts.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, String, Table, and_, func, insert, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from tracepilot.models import DecisionState, InvestigationScope, fingerprint, utc_now
from tracepilot.oncall import OnCallConfig, Source, Webhook
from tracepilot.store import EvidenceStore, StaleCheckpoint


class LeaseLost(StaleCheckpoint):
    """The task was cancelled, reclaimed, or its worker stopped renewing."""


class TaskConflict(ValueError):
    pass


class TaskStore:
    def __init__(self, evidence: EvidenceStore, config: OnCallConfig, clock: Callable[[], float] = time.time):
        self.evidence, self.engine, self.config, self.clock = evidence, evidence.engine, config, clock
        metadata = evidence.metadata
        self.incidents = Table("tracepilot_incidents", metadata,
            Column("id", String(36), primary_key=True), Column("tenant_id", String(100), nullable=False, index=True),
            Column("source_id", String(100), nullable=False), Column("active_key", String(64), unique=True),
            Column("status", String(20), nullable=False), Column("revision", Integer, nullable=False),
            Column("scope", JSON, nullable=False), Column("context", JSON, nullable=False),
            Column("active_task_id", String(36)), Column("updated_at", Float, nullable=False))
        self.alerts = Table("tracepilot_alert_occurrences", metadata,
            Column("id", String(64), primary_key=True), Column("tenant_id", String(100), nullable=False),
            Column("incident_id", String(36), ForeignKey(self.incidents.c.id), index=True),
            Column("status", String(20), nullable=False), Column("record", JSON, nullable=False),
            Column("payload_hash", String(64), nullable=False), Column("first_seen", Float, nullable=False),
            Column("last_seen", Float, nullable=False))
        self.jobs = Table("tracepilot_task_queue", metadata,
            Column("id", String(36), ForeignKey(evidence.tasks.c.id), primary_key=True),
            Column("tenant_id", String(100), nullable=False, index=True), Column("source_id", String(100), nullable=False),
            Column("incident_id", String(36), ForeignKey(self.incidents.c.id), index=True),
            Column("queue_state", String(20), nullable=False, index=True), Column("ready_at", Float, nullable=False),
            Column("lease_epoch", Integer, nullable=False), Column("lease_until", Float, nullable=False),
            Column("owner", String(100)), Column("wait_until", Float), Column("created_at", Float, nullable=False))
        self.receipts = Table("tracepilot_alert_receipts", metadata,
            Column("id", String(36), primary_key=True), Column("source_id", String(100), nullable=False),
            Column("tenant_id", String(100), nullable=False), Column("received_at", Float, nullable=False),
            Column("payload_hash", String(64), nullable=False), Column("record", JSON, nullable=False))
        self.schema = Table("tracepilot_schema_versions", metadata,
            Column("version", Integer, primary_key=True), Column("applied_at", Float, nullable=False))

    def initialize(self) -> None:
        # v1 is additive: existing CLI task/evidence/event tables are not altered.
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                conn.execute(text("SELECT pg_advisory_xact_lock(841072301)"))
            self.evidence.metadata.create_all(conn)
            latest = conn.execute(select(self.schema.c.version).order_by(self.schema.c.version.desc()).limit(1)).scalar()
            if latest is not None and latest > 1:
                raise ValueError("Database schema is newer than this worker")
            self._insert_once(conn, self.schema, {"version": 1, "applied_at": self.clock()}, self.schema.c.version)

    @staticmethod
    def _insert_once(conn: Connection, table: Table, values: dict[str, Any], key: Any) -> bool:
        dialect = conn.dialect.name
        constructor = pg_insert if dialect == "postgresql" else sqlite_insert if dialect == "sqlite" else None
        if constructor is None:
            raise ValueError("OnCall requires PostgreSQL or SQLite")
        result = conn.execute(constructor(table).values(**values).on_conflict_do_nothing().returning(key)).first()
        return result is not None

    def _state(self, conn: Connection, task_id: str) -> DecisionState:
        row = conn.execute(select(self.evidence.tasks.c.state).where(self.evidence.tasks.c.id == task_id)).first()
        if row is None:
            raise KeyError("Unknown task")
        return DecisionState.model_validate(row[0])

    def _new_task(self, conn: Connection, incident: dict[str, Any], source: Source, question: str | None = None) -> str:
        scope = InvestigationScope.model_validate(incident["scope"])
        state = DecisionState(question=question or f"调查 {scope.service} 告警，区分事实、候选原因和待核验项。",
            context={"incident": incident["context"], "tenant_id": source.tenant_id, "cluster": source.cluster},
            scope=scope, budget=source.budget.model_copy(deep=True), status="queued",
            incident_revision=incident["revision"], allowed_services=source.authorized_services(scope.service))
        self.evidence.create(state, connection=conn)
        self.evidence.checkpoint(state, event={"kind": "queued", "task_id": state.task_id}, connection=conn)
        conn.execute(insert(self.jobs).values(id=state.task_id, tenant_id=source.tenant_id,
            source_id=incident["source_id"], incident_id=incident["id"], queue_state="ready", ready_at=self.clock(),
            lease_epoch=0, lease_until=0, created_at=self.clock()))
        conn.execute(update(self.incidents).where(self.incidents.c.id == incident["id"]).values(active_task_id=state.task_id))
        return state.task_id

    def ingest(self, source_id: str, payload: Webhook) -> dict[str, Any]:
        source = self.config.sources[source_id]
        now = self.clock()
        received = datetime.fromtimestamp(now, timezone.utc)
        normalized = []
        for alert in payload.alerts:
            service = source.service_aliases.get(alert.labels.get(source.service_label, ""), alert.labels.get(source.service_label, ""))
            namespace = alert.labels.get(source.namespace_label, "")
            if service not in source.services or namespace not in source.namespaces:
                raise PermissionError("Alert service/namespace is not in the source allowlist")
            if alert.startsAt > received + timedelta(seconds=60):
                raise ValueError("Alert startsAt is in the future")
            record = {"source_group": source.source_group, "tenant_id": source.tenant_id, "cluster": source.cluster,
                      "service": service, "namespace": namespace, "alert": alert.model_dump(mode="json")}
            # Firing endsAt is a moving expiry estimate, not a new observation.
            if alert.status == "firing":
                record["alert"]["endsAt"] = None
            key = fingerprint([source.tenant_id, source.source_group, source.cluster, alert.fingerprint, alert.startsAt.isoformat()])
            correlation = fingerprint([source.tenant_id, source.source_group, source.cluster, namespace, service,
                                       {label: alert.labels.get(label, "") for label in source.correlation_labels}])
            normalized.append((key, correlation, record, alert))
        result: dict[str, Any] = {"task_ids": [], "created_tasks": 0, "updated_alerts": 0, "duplicates": 0,
                                  "truncated_alerts": payload.truncatedAlerts}
        with self.engine.begin() as conn:
            receipt_id = str(uuid4())
            conn.execute(insert(self.receipts).values(id=receipt_id, source_id=source_id, tenant_id=source.tenant_id,
                received_at=now, payload_hash=fingerprint(payload.model_dump(mode="json")),
                record={"count": len(normalized), "truncated_alerts": payload.truncatedAlerts}))
            for key, correlation, record, alert in normalized:
                digest = fingerprint(record)
                created = self._insert_once(conn, self.alerts, {"id": key, "tenant_id": source.tenant_id,
                    "status": alert.status, "record": record, "payload_hash": digest, "first_seen": now, "last_seen": now}, self.alerts.c.id)
                row = dict(conn.execute(select(self.alerts).where(self.alerts.c.id == key).with_for_update()).mappings().one())
                conn.execute(update(self.alerts).where(self.alerts.c.id == key).values(last_seen=now))
                if (not created and (row["payload_hash"] == digest or row["status"] == "resolved" and alert.status == "firing")):
                    result["duplicates"] += 1
                    if row["incident_id"]:
                        conn.execute(update(self.incidents).where(self.incidents.c.id == row["incident_id"]).values(updated_at=now))
                        active = conn.execute(select(self.incidents.c.active_task_id).where(self.incidents.c.id == row["incident_id"])).scalar()
                        if active:
                            result["task_ids"].append(active)
                    continue
                incident_id = row["incident_id"]
                if incident_id is None and alert.status == "firing":
                    start = max(alert.startsAt - timedelta(seconds=source.lookback_seconds),
                                received - timedelta(seconds=source.max_window_seconds))
                    scope = InvestigationScope(service=record["service"], namespace=record["namespace"], start=start, end=received)
                    candidate_id = str(uuid4())
                    previous = conn.execute(select(self.incidents).where(
                        self.incidents.c.active_key == correlation).with_for_update()).mappings().first()
                    if previous and previous["updated_at"] < now - source.correlation_window_seconds:
                        conn.execute(update(self.incidents).where(self.incidents.c.id == previous["id"]).values(
                            active_key=None, status="stale"))
                    self._insert_once(conn, self.incidents, {"id": candidate_id, "tenant_id": source.tenant_id,
                        "source_id": source_id, "active_key": correlation, "status": "firing", "revision": 0,
                        "scope": scope.model_dump(mode="json"), "context": {}, "updated_at": now}, self.incidents.c.id)
                    incident_id = conn.execute(select(self.incidents.c.id).where(self.incidents.c.active_key == correlation)).scalar_one()
                conn.execute(update(self.alerts).where(self.alerts.c.id == key).values(
                    incident_id=incident_id, status=alert.status, record=record, payload_hash=digest))
                result["updated_alerts"] += 1
                if incident_id is None:
                    continue  # Resolved-before-firing tombstone; never open a diagnosis for it.
                incident = dict(conn.execute(select(self.incidents).where(self.incidents.c.id == incident_id).with_for_update()).mappings().one())
                observations = conn.execute(select(self.alerts.c.record).where(self.alerts.c.incident_id == incident_id)
                    .order_by(self.alerts.c.first_seen.desc()).limit(101)).scalars().all()
                active_count = conn.execute(select(self.alerts.c.id).where(
                    self.alerts.c.incident_id == incident_id, self.alerts.c.status == "firing").limit(1)).first()
                incident["revision"] += 1
                incident["status"] = "firing" if active_count else "unknown" if payload.truncatedAlerts else "resolved"
                summaries = [{"service": item["service"], "namespace": item["namespace"],
                    "cluster": item["cluster"], "status": item["alert"]["status"],
                    "starts_at": item["alert"]["startsAt"], "ends_at": item["alert"]["endsAt"],
                    "fingerprint": item["alert"]["fingerprint"],
                    "labels": {k: item["alert"]["labels"][k][:200] for k in ["alertname", "pod", "severity"]
                               if k in item["alert"]["labels"]},
                    "summary": item["alert"]["annotations"].get("summary", "")[:500]}
                    for item in observations[:100]]
                incident["context"] = {"alerts": summaries, "alerts_truncated": len(observations) > 100,
                    "notification_truncated": payload.truncatedAlerts > 0, "alert_status": incident["status"],
                    "revision": incident["revision"]}
                # A follow-up investigation gets a fresh fixed time window.
                current_scope = InvestigationScope.model_validate(incident["scope"])
                latest_end = max(current_scope.end, received)
                incident["scope"] = current_scope.model_copy(update={
                    "start": max(current_scope.start, latest_end - timedelta(seconds=source.max_window_seconds)),
                    "end": latest_end,
                }).model_dump(mode="json")
                conn.execute(update(self.incidents).where(self.incidents.c.id == incident_id).values(
                    revision=incident["revision"], context=incident["context"], scope=incident["scope"],
                    status=incident["status"], active_key=incident["active_key"] if incident["status"] != "resolved" else None, updated_at=now))
                task_id = incident["active_task_id"]
                if task_id is None:
                    task_id = self._new_task(conn, incident, source)
                    result["created_tasks"] += 1
                result["task_ids"].append(task_id)
            conn.execute(update(self.receipts).where(self.receipts.c.id == receipt_id).values(record={
                "count": len(normalized), "truncated_alerts": payload.truncatedAlerts,
                "created_tasks": result["created_tasks"], "updated_alerts": result["updated_alerts"],
                "duplicates": result["duplicates"],
            }))
        result["task_ids"] = list(dict.fromkeys(result["task_ids"]))
        return result

    def submit(self, source_id: str, scope: InvestigationScope, question: str) -> str:
        source = self.config.sources[source_id]
        if scope.service not in source.services or scope.namespace not in source.namespaces:
            raise PermissionError("Manual investigation exceeds source scope")
        incident = {"id": str(uuid4()), "tenant_id": source.tenant_id, "source_id": source_id, "active_key": None,
                    "status": "manual", "revision": 1, "scope": scope.model_dump(mode="json"), "context": {"manual": True},
                    "active_task_id": None, "updated_at": self.clock()}
        with self.engine.begin() as conn:
            conn.execute(insert(self.incidents).values(**incident))
            return self._new_task(conn, incident, source, question)

    def claim(self, owner: str) -> dict[str, Any] | None:
        now = self.clock()
        eligible = and_(self.jobs.c.ready_at <= now, or_(self.jobs.c.queue_state == "ready",
                    and_(self.jobs.c.queue_state == "running", self.jobs.c.lease_until <= now)))
        with self.engine.begin() as conn:
            row = conn.execute(select(self.jobs).where(eligible).order_by(self.jobs.c.created_at)
                .with_for_update(skip_locked=True).limit(1)).mappings().first()
            if row is None:
                return None
            token = dict(row)
            changed = conn.execute(update(self.jobs).where(self.jobs.c.id == row["id"], eligible,
                self.jobs.c.lease_epoch == row["lease_epoch"]).values(queue_state="running", owner=owner,
                lease_epoch=row["lease_epoch"] + 1, lease_until=now + self.config.lease_seconds))
            if changed.rowcount != 1:
                return None
            token.update(owner=owner, lease_epoch=row["lease_epoch"] + 1)
            state = self._state(conn, row["id"])
            if state.status == "queued":
                state.status = "running"
            elif state.status != "running":
                # Runtime may have committed a final report just before its worker died.
                self._finish(conn, token, state)
                return None
            self.evidence.checkpoint(state, event={"kind": "claimed", "lease_epoch": token["lease_epoch"]}, connection=conn)
            return token

    def guard(self, conn: Connection, token: dict[str, Any]) -> None:
        valid = conn.execute(select(self.jobs.c.id).where(self.jobs.c.id == token["id"],
            self.jobs.c.queue_state == "running", self.jobs.c.owner == token["owner"],
            self.jobs.c.lease_epoch == token["lease_epoch"], self.jobs.c.lease_until > self.clock()).with_for_update()).first()
        if valid is None:
            raise LeaseLost("Task lease is no longer owned")

    def heartbeat(self, token: dict[str, Any]) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(update(self.jobs).where(self.jobs.c.id == token["id"],
                self.jobs.c.queue_state == "running", self.jobs.c.owner == token["owner"],
                self.jobs.c.lease_epoch == token["lease_epoch"], self.jobs.c.lease_until > self.clock())
                .values(lease_until=self.clock() + self.config.lease_seconds))
            return result.rowcount == 1

    def sync_context(self, token: dict[str, Any], state: DecisionState) -> None:
        with self.engine.begin() as conn:
            self.guard(conn, token)
            incident = conn.execute(select(self.incidents).where(self.incidents.c.id == token["incident_id"])).mappings().one()
            if incident["revision"] > state.incident_revision:
                latest_scope = InvestigationScope.model_validate(incident["scope"])
                if state.scope is not None and latest_scope.end > state.scope.end:
                    # Keep each task's observation window immutable. Finish will
                    # create one follow-up for the unprocessed incident revision.
                    if state.context.get("pending_incident_revision") != incident["revision"]:
                        state.context["pending_incident_revision"] = incident["revision"]
                        self.evidence.checkpoint(state, event={"kind": "followup_context_pending"}, connection=conn)
                    return
                state.context["incident"] = incident["context"]
                state.incident_revision = incident["revision"]
                state.candidates, state.groups = [], []
                state.context["replan_required"] = True
                self.evidence.checkpoint(state, event={"kind": "incident_updated", "revision": state.incident_revision}, connection=conn)

    def _finish(self, conn: Connection, token: dict[str, Any], state: DecisionState) -> None:
        waiting = state.status == "needs_input"
        conn.execute(update(self.jobs).where(self.jobs.c.id == token["id"]).values(
            queue_state="waiting" if waiting else "terminal", owner=None, lease_until=0,
            wait_until=self.clock() + self.config.input_wait_seconds if waiting else None))
        incident = dict(conn.execute(select(self.incidents).where(
            self.incidents.c.id == token["incident_id"]).with_for_update()).mappings().one())
        if not waiting:
            conn.execute(update(self.incidents).where(self.incidents.c.id == incident["id"],
                self.incidents.c.active_task_id == token["id"]).values(active_task_id=None))
            if incident["revision"] > state.incident_revision and state.status != "cancelled":
                self._new_task(conn, incident, self.config.sources[incident["source_id"]])

    def finish(self, token: dict[str, Any], state: DecisionState) -> None:
        with self.engine.begin() as conn:
            self.guard(conn, token)
            self._finish(conn, token, state)

    def job(self, task_id: str, tenant_id: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(select(self.jobs).where(self.jobs.c.id == task_id, self.jobs.c.tenant_id == tenant_id)).mappings().first()
        if row is None:
            raise KeyError("Unknown task")
        return dict(row)

    def incident(self, incident_id: str, tenant_id: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(select(self.incidents).where(self.incidents.c.id == incident_id,
                self.incidents.c.tenant_id == tenant_id)).mappings().first()
            if row is None:
                raise KeyError("Unknown incident")
            tasks = conn.execute(select(self.jobs.c.id).where(self.jobs.c.incident_id == incident_id)
                .order_by(self.jobs.c.created_at).limit(100)).scalars().all()
            return {**dict(row), "task_ids": tasks}

    def metrics(self, tenant_id: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            counts = conn.execute(select(self.jobs.c.queue_state, func.count()).where(self.jobs.c.tenant_id == tenant_id)
                                 .group_by(self.jobs.c.queue_state)).all()
            oldest = conn.execute(select(func.min(self.jobs.c.ready_at)).where(
                self.jobs.c.tenant_id == tenant_id, self.jobs.c.queue_state == "ready")).scalar()
            receipts = conn.execute(select(func.count()).select_from(self.receipts).where(self.receipts.c.tenant_id == tenant_id)).scalar()
        return {"tasks_by_queue_state": dict(counts), "oldest_ready_seconds": max(0, self.clock() - oldest) if oldest is not None else 0,
                "accepted_webhooks": receipts}

    def control(self, task_id: str, tenant_id: str, *, cancel: bool = False, input_text: str = "") -> None:
        with self.engine.begin() as conn:
            job = conn.execute(select(self.jobs).where(self.jobs.c.id == task_id,
                self.jobs.c.tenant_id == tenant_id).with_for_update()).mappings().first()
            if job is None:
                raise KeyError("Unknown task")
            state = self._state(conn, task_id)
            if cancel:
                if job["queue_state"] == "terminal" or state.status in {"completed", "stopped", "failed", "cancelled"}:
                    return
                state.status, state.pending_status, state.stop_reason = "cancelled", None, "operator_cancelled"
                conn.execute(update(self.jobs).where(self.jobs.c.id == task_id).values(
                    queue_state="terminal", owner=None, lease_until=0, lease_epoch=job["lease_epoch"] + 1))
                conn.execute(update(self.incidents).where(self.incidents.c.id == job["incident_id"],
                    self.incidents.c.active_task_id == task_id).values(active_task_id=None))
            else:
                if job["queue_state"] != "waiting" or job["wait_until"] <= self.clock():
                    raise TaskConflict("Only an unexpired needs_input task can resume")
                if state.deadline_at is not None and state.deadline_at <= utc_now():
                    raise TaskConflict("Execution deadline exhausted; create a new investigation")
                state.status, state.pending_status, state.stop_reason = "queued", None, ""
                state.context["operator_input"] = input_text
                state.context["replan_required"] = True
                state.candidates, state.groups = [], []
                conn.execute(update(self.evidence.tasks).where(self.evidence.tasks.c.id == task_id).values(diagnosis=None))
                conn.execute(update(self.jobs).where(self.jobs.c.id == task_id).values(
                    queue_state="ready", ready_at=self.clock(), owner=None, lease_until=0,
                    lease_epoch=job["lease_epoch"] + 1, wait_until=None))
            self.evidence.checkpoint(state, event={"kind": "cancelled" if cancel else "operator_resumed"}, connection=conn)

    def expire_waiting(self) -> int:
        with self.engine.begin() as conn:
            rows = conn.execute(select(self.jobs).where(self.jobs.c.queue_state == "waiting",
                self.jobs.c.wait_until <= self.clock()).with_for_update(skip_locked=True).limit(100)).mappings().all()
            for row in rows:
                changed = conn.execute(update(self.jobs).where(self.jobs.c.id == row["id"],
                    self.jobs.c.queue_state == "waiting", self.jobs.c.wait_until <= self.clock()).values(
                    queue_state="terminal", owner=None, lease_until=0, lease_epoch=row["lease_epoch"] + 1))
                if changed.rowcount != 1:
                    continue
                state = self._state(conn, row["id"])
                state.status, state.pending_status, state.stop_reason = "stopped", None, "input_wait_timeout"
                conn.execute(update(self.incidents).where(self.incidents.c.id == row["incident_id"],
                    self.incidents.c.active_task_id == row["id"]).values(active_task_id=None))
                self.evidence.checkpoint(state, event={"kind": "input_wait_expired"}, connection=conn)
            return len(rows)


class LeasedEvidenceStore(EvidenceStore):
    """A per-task view; never change a shared store's current lease."""

    def __init__(self, tasks: TaskStore, token: dict[str, Any]):
        super().__init__(tasks.engine)
        self.tasks_store, self.token = tasks, token

    def check_write(self, conn: Connection, state: DecisionState) -> None:
        if state.task_id != self.token["id"]:
            raise LeaseLost("Lease belongs to another task")
        self.tasks_store.guard(conn, self.token)
