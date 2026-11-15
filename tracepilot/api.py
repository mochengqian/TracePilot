"""Authenticated ingress and task APIs; model execution belongs to the worker."""

import hmac
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, TypeVar

import anyio
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.exc import SQLAlchemyError

from tracepilot.models import InvestigationScope, Record
from tracepilot.oncall import OnCallConfig, Webhook
from tracepilot.store import EvidenceStore
from tracepilot.task_store import TaskConflict, TaskStore


class ManualInvestigation(Record):
    source_id: str = Field(min_length=1, max_length=100)
    scope: InvestigationScope
    question: str = Field(min_length=1, max_length=16000)


class ResumeInput(Record):
    input_text: str = Field(min_length=1, max_length=16000)


T = TypeVar("T", bound=BaseModel)


def create_app(config: OnCallConfig, tasks: TaskStore | None = None) -> FastAPI:
    config.validate_secrets()
    owns_store = tasks is None
    if tasks is None:
        url = os.environ.get(config.database_url_env)
        if not url:
            raise ValueError(f"Required environment variable is missing: {config.database_url_env}")
        tasks = TaskStore(EvidenceStore.from_url(url), config)
    tasks.initialize()
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            if owns_store:
                tasks.engine.dispose()

    app = FastAPI(title="TracePilot OnCall", version="1", docs_url=None, redoc_url=None, lifespan=lifespan)
    app.state.tasks = tasks

    def authenticate(request: Request, token_env: str) -> bool:
        value = request.headers.get("authorization", "")
        expected = os.environ.get(token_env, "")
        return bool(expected and value.startswith("Bearer ") and len(value) <= 4096
                    and hmac.compare_digest(value[7:].encode(), expected.encode()))

    def tenant(request: Request) -> str:
        for principal in config.principals.values():
            if authenticate(request, principal.token_env):
                return principal.tenant_id
        raise HTTPException(401, "A valid API bearer token is required")

    Tenant = Annotated[str, Depends(tenant)]

    async def body(request: Request, model: type[T]) -> T:
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > config.max_body_bytes:
                raise HTTPException(413, "Request body exceeds configured limit")
        try:
            return model.model_validate_json(data)
        except ValidationError:
            raise HTTPException(422, "Invalid request payload; check fields, scope and timezone-aware timestamps") from None

    @app.exception_handler(KeyError)
    async def not_found(request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse({"detail": "Unknown task, incident, source or evidence"}, status_code=404)

    @app.exception_handler(TaskConflict)
    async def conflict(request: Request, exc: TaskConflict) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(PermissionError)
    async def forbidden(request: Request, exc: PermissionError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=403)

    @app.exception_handler(SQLAlchemyError)
    async def unavailable(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        return JSONResponse({"detail": "Task persistence is temporarily unavailable"}, status_code=503)

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/webhooks/alertmanager/{source_id}")
    async def alertmanager(source_id: str, request: Request) -> JSONResponse:
        source = config.sources.get(source_id)
        if source is None or not authenticate(request, source.token_env):
            raise HTTPException(401, "A valid source bearer token is required")
        payload = await body(request, Webhook)
        try:
            result = await anyio.to_thread.run_sync(lambda: tasks.ingest(source_id, payload))
        except ValueError:
            raise HTTPException(422, "Invalid alert occurrence or investigation window") from None
        return JSONResponse(result, status_code=202 if result["updated_alerts"] else 200)

    @app.post("/api/v1/investigations", status_code=202)
    async def investigate(request: Request, tenant_id: Tenant) -> dict[str, str]:
        payload = await body(request, ManualInvestigation)
        source = config.sources.get(payload.source_id)
        if source is None or source.tenant_id != tenant_id:
            raise HTTPException(404, "Unknown source")
        task_id = await anyio.to_thread.run_sync(lambda: tasks.submit(payload.source_id, payload.scope, payload.question))
        return {"task_id": task_id}

    @app.get("/api/v1/tasks/{task_id}")
    def task(task_id: str, tenant_id: Tenant) -> dict[str, Any]:
        job = tasks.job(task_id, tenant_id)
        state = tasks.evidence.load(task_id)
        diagnosis = tasks.evidence.load_diagnosis(task_id)
        return {"task_id": task_id, "incident_id": job["incident_id"], "state": state.model_dump(mode="json"),
                "diagnosis": diagnosis.model_dump(mode="json") if diagnosis else None}

    @app.get("/api/v1/incidents/{incident_id}")
    def incident(incident_id: str, tenant_id: Tenant) -> dict[str, Any]:
        return tasks.incident(incident_id, tenant_id)

    @app.get("/api/v1/metrics")
    def metrics(tenant_id: Tenant) -> dict[str, Any]:
        return tasks.metrics(tenant_id)

    @app.get("/api/v1/tasks/{task_id}/evidence/{evidence_id}")
    def evidence(task_id: str, evidence_id: str, tenant_id: Tenant,
                 offset: int = Query(0, ge=0), limit: int = Query(4000, ge=1, le=12000)) -> dict[str, Any]:
        tasks.job(task_id, tenant_id)
        return tasks.evidence.read_evidence(task_id, evidence_id, offset, limit)

    @app.post("/api/v1/tasks/{task_id}/resume", status_code=202)
    async def resume(task_id: str, request: Request, tenant_id: Tenant) -> dict[str, str]:
        payload = await body(request, ResumeInput)
        await anyio.to_thread.run_sync(lambda: tasks.control(task_id, tenant_id, input_text=payload.input_text))
        return {"task_id": task_id}

    @app.post("/api/v1/tasks/{task_id}/cancel", status_code=202)
    def cancel(task_id: str, tenant_id: Tenant) -> dict[str, str]:
        tasks.control(task_id, tenant_id, cancel=True)
        return {"task_id": task_id}

    @app.get("/api/v1/tasks/{task_id}/events")
    async def events(task_id: str, request: Request, tenant_id: Tenant,
                     after: int = Query(0, ge=0), follow: bool = True) -> StreamingResponse:
        await anyio.to_thread.run_sync(lambda: tasks.job(task_id, tenant_id))
        try:
            cursor = max(after, int(request.headers.get("last-event-id", "0")))
            if cursor < 0:
                raise ValueError
        except ValueError:
            raise HTTPException(400, "Last-Event-ID must be a nonnegative integer") from None

        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                page = await anyio.to_thread.run_sync(lambda: tasks.evidence.events_since(task_id, cursor))
                for event in page:
                    cursor = event["id"]
                    yield f"id: {cursor}\nevent: {event['kind']}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                if len(page) == 100:
                    continue
                state = await anyio.to_thread.run_sync(lambda: tasks.evidence.load(task_id))
                if not follow or state.status not in {"queued", "running"}:
                    break
                yield ": heartbeat\n\n"
                await anyio.sleep(1)

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


def configured_app() -> FastAPI:
    path = os.environ.get("TRACEPILOT_ONCALL_CONFIG")
    if not path:
        raise ValueError("TRACEPILOT_ONCALL_CONFIG is required")
    return create_app(OnCallConfig.load(Path(path)))
