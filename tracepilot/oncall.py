"""Operator-owned OnCall configuration and bounded Alertmanager contracts."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from tracepilot.models import Budget, Record


class Alert(Record):
    status: Literal["firing", "resolved"]
    labels: dict[str, str] = Field(min_length=1, max_length=100)
    annotations: dict[str, str] = Field(default_factory=dict, max_length=100)
    startsAt: datetime
    endsAt: datetime | None = None
    fingerprint: str = Field(min_length=1, max_length=256)
    generatorURL: str = Field(default="", max_length=4096)

    @model_validator(mode="after")
    def bounded(self) -> "Alert":
        if self.startsAt.utcoffset() is None or (self.endsAt is not None and self.endsAt.utcoffset() is None):
            raise ValueError("Alert timestamps require a timezone")
        if self.startsAt.year < 1970:
            raise ValueError("A real startsAt is required for occurrence deduplication")
        if self.status == "resolved" and (self.endsAt is None or self.endsAt < self.startsAt):
            raise ValueError("Resolved alerts require endsAt >= startsAt")
        if any(len(k) > 200 or len(v) > 8000 for d in (self.labels, self.annotations) for k, v in d.items()):
            raise ValueError("Alert label or annotation is too large")
        self.startsAt = self.startsAt.astimezone(timezone.utc)
        if self.endsAt is not None:
            self.endsAt = self.endsAt.astimezone(timezone.utc)
        return self


class Webhook(BaseModel):
    model_config = ConfigDict(extra="ignore")
    alerts: list[Alert] = Field(min_length=1, max_length=1000)
    groupKey: str = Field(default="", max_length=4096)
    truncatedAlerts: int = Field(default=0, ge=0)


class Source(Record):
    tenant_id: str = Field(min_length=1, max_length=100)
    source_group: str = Field(min_length=1, max_length=100)
    cluster: str = Field(min_length=1, max_length=100)
    token_env: str = Field(min_length=1)
    agent_config: Path | None = None
    demo_scenario: Literal["db-pool", "slow-dependency", "no-data", "denied"] | None = None
    namespaces: list[str] = Field(min_length=1, max_length=100)
    services: list[str] = Field(min_length=1, max_length=1000)
    service_label: str = "service"
    namespace_label: str = "namespace"
    service_aliases: dict[str, str] = Field(default_factory=dict, max_length=1000)
    dependencies: dict[str, list[str]] = Field(default_factory=dict, max_length=1000)
    correlation_labels: list[str] = Field(default_factory=lambda: ["alertname", "pod"], max_length=10)
    correlation_window_seconds: int = Field(default=900, ge=1, le=86400)
    lookback_seconds: int = Field(default=300, ge=1, le=3600)
    max_window_seconds: int = Field(default=3600, ge=60, le=86400)
    budget: Budget = Field(default_factory=Budget)

    @model_validator(mode="after")
    def valid_policy(self) -> "Source":
        if not self.service_label or not self.namespace_label or self.service_label == self.namespace_label:
            raise ValueError("Service and namespace bindings must be distinct")
        if any(not v or len(v) > 200 for v in [*self.services, *self.namespaces]):
            raise ValueError("Scope names must be nonempty and at most 200 characters")
        if (self.agent_config is None) == (self.demo_scenario is None):
            raise ValueError("Choose exactly one agent_config or explicit demo_scenario")
        if any(v not in self.services for v in self.service_aliases.values()):
            raise ValueError("Service aliases must target authorized services")
        if any(k not in self.services or any(v not in self.services for v in values)
               for k, values in self.dependencies.items()):
            raise ValueError("Dependencies must be explicitly authorized services")
        return self

    def authorized_services(self, service: str) -> list[str]:
        return list(dict.fromkeys([service, *self.dependencies.get(service, [])]))


class Principal(Record):
    tenant_id: str = Field(min_length=1, max_length=100)
    token_env: str = Field(min_length=1)


class OnCallConfig(Record):
    database_url_env: str = "TRACEPILOT_DATABASE_URL"
    sources: dict[str, Source] = Field(min_length=1, max_length=100)
    principals: dict[str, Principal] = Field(min_length=1, max_length=100)
    lease_seconds: float = Field(default=60, ge=3, le=600)
    poll_seconds: float = Field(default=1, ge=0.05, le=30)
    input_wait_seconds: float = Field(default=3600, ge=1, le=86400)
    max_body_bytes: int = Field(default=2_000_000, ge=1000, le=10_000_000)

    @classmethod
    def load(cls, path: Path) -> "OnCallConfig":
        if path.stat().st_size > 1_000_000:
            raise ValueError("OnCall configuration is too large")
        config = cls.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        for source in config.sources.values():
            if source.agent_config is not None and not source.agent_config.is_absolute():
                source.agent_config = (path.parent / source.agent_config).resolve()
        return config

    def validate_secrets(self) -> None:
        values = []
        for item in [*self.sources.values(), *self.principals.values()]:
            token = os.environ.get(item.token_env, "")
            if len(token) < 16:
                raise ValueError(f"Configure a token of at least 16 characters in {item.token_env}")
            values.append((item.tenant_id, token))
        # A credential must never silently select between different tenants.
        owners: dict[str, str] = {}
        for tenant, token in values:
            if token in owners and owners[token] != tenant:
                raise ValueError("A credential cannot be shared by different tenants")
            owners[token] = tenant
