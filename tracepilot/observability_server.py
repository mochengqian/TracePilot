"""Read-only MCP tools for configured Prometheus, Loki, Tempo and Kubernetes APIs."""

import json
import os
import re
from typing import Annotated, Any, Literal
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from tracepilot.config import validate_endpoint
from tracepilot.models import InvestigationScope, utc_now
from tracepilot.limits import remaining_timeout


class ObservabilityClient:
    def __init__(self, environment: dict[str, str]):
        self.env = environment

    def get(self, backend: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
        base = self.env.get(f"{backend}_URL", "")
        validate_endpoint(base)
        headers = {"Accept": "application/json"}
        if self.env.get(f"{backend}_TOKEN"):
            headers["Authorization"] = "Bearer " + self.env[f"{backend}_TOKEN"]
        if self.env.get(f"{backend}_TENANT"):
            headers["X-Scope-OrgID"] = self.env[f"{backend}_TENANT"]
        with requests.get(base.rstrip("/") + path, params=params, headers=headers,
                          timeout=(remaining_timeout(3), remaining_timeout(12)),
                          verify=self.env.get(f"{backend}_CA_BUNDLE") or True, allow_redirects=False, stream=True) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError(f"{backend} returned unexpected HTTP {response.status_code}")
            chunks = []
            size = 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > 2_000_000:
                    raise ValueError(f"{backend} response exceeds 2 MB; narrow the query")
                chunks.append(chunk)
            data = json.loads(b"".join(chunks))
        if not isinstance(data, dict):
            raise ValueError(f"{backend} returned an invalid JSON object")
        return data

    def labels(self, service: str, namespace: str, pod: str | None = None) -> str:
        names = [self.env.get("OBS_SERVICE_LABEL", "service"), self.env.get("OBS_NAMESPACE_LABEL", "namespace")]
        if pod:
            names.append(self.env.get("OBS_POD_LABEL", "pod"))
        if len(set(names)) != len(names):
            raise ValueError("Configured telemetry label names must be distinct")
        fields = dict(zip(names, [service, namespace, *([pod] if pod else [])]))
        if any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name) for name in names):
            raise ValueError("Invalid configured telemetry label name")
        return "{" + ",".join(f"{key}={json.dumps(value)}" for key, value in fields.items()) + "}"


def scope_for(service: str, namespace: str, start: str, end: str) -> InvestigationScope:
    return InvestigationScope(service=service, namespace=namespace, start=start, end=end)


def build_server(environment: dict[str, str] | None = None) -> FastMCP:
    env = dict(os.environ) if environment is None else environment
    client = ObservabilityClient(env)
    server = FastMCP("TracePilot Observability")

    if env.get("LOKI_URL"):
        @server.tool()
        def service_logs(service: str, namespace: str, start: str, end: str, pod: str | None = None,
                         contains: str = "", limit: Annotated[int, Field(ge=1, le=100)] = 50) -> dict[str, Any]:
            """Read scoped Loki logs. contains is a literal substring, never a LogQL program."""
            scope = scope_for(service, namespace, start, end)
            query = client.labels(service, namespace, pod)
            if contains:
                query += " |= " + json.dumps(contains)
            result = client.get("LOKI", "/loki/api/v1/query_range", {
                "query": query, "start": scope.start.isoformat(), "end": scope.end.isoformat(), "limit": limit, "direction": "forward"})
            if result.get("status") != "success":
                raise ValueError("Loki query did not succeed")
            return {"source": "loki", "scope": scope.model_dump(mode="json"), "query": query, "result": result}

    if env.get("PROMETHEUS_URL"):
        @server.tool()
        def service_metric_summary(service: str, namespace: str, start: str, end: str,
                                   signal: Literal["latency_p95", "request_rate", "error_rate"],
                                   step_seconds: Annotated[int, Field(ge=15, le=3600)] = 60) -> dict[str, Any]:
            """Query operator-defined HTTP metric templates, never arbitrary PromQL."""
            scope = scope_for(service, namespace, start, end)
            if (scope.end - scope.start).total_seconds() / step_seconds > 1440:
                raise ValueError("Too many time points")
            duration = env.get("OBS_HTTP_DURATION_BUCKET", "http_request_duration_seconds_bucket")
            requests_metric = env.get("OBS_HTTP_REQUESTS_TOTAL", "http_requests_total")
            status_label = env.get("OBS_HTTP_STATUS_LABEL", "status")
            if any(not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", n) for n in [duration, requests_metric]):
                raise ValueError("Metric templates require valid metric names")
            if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", status_label):
                raise ValueError("Invalid HTTP status label")
            selectors = client.labels(service, namespace)
            query_start = scope.start + timedelta(seconds=300)
            if query_start >= scope.end:
                raise ValueError("HTTP rate templates require a window longer than five minutes")
            total = f"sum(rate({requests_metric}{selectors}[5m]))"
            if signal == "latency_p95":
                query = f"histogram_quantile(0.95,sum by (le)(rate({duration}{selectors}[5m])))"
            elif signal == "request_rate":
                query = total
            else:
                if status_label in {env.get("OBS_SERVICE_LABEL", "service"), env.get("OBS_NAMESPACE_LABEL", "namespace")}:
                    raise ValueError("HTTP status label cannot replace a scope label")
                errors = selectors[:-1] + f',{status_label}=~"5.."' + "}"
                query = f"sum(rate({requests_metric}{errors}[5m])) / clamp_min({total},1e-9)"
            result = client.get("PROMETHEUS", "/api/v1/query_range", {"query": query,
                "start": query_start.isoformat(), "end": scope.end.isoformat(), "step": step_seconds, "limit": 100, "timeout": "10s"})
            if result.get("status") != "success":
                raise ValueError("Prometheus query did not succeed")
            return {"source": "prometheus", "scope": scope.model_dump(mode="json"), "query": query, "result": result,
                    "template": signal, "rate_lookback_seconds": 300, "query_start": query_start.isoformat(),
                    "observation_status": "no_data" if result.get("data", {}).get("result") == [] else "success",
                    "collected_at": utc_now().isoformat()}

        @server.tool()
        def service_metrics(service: str, namespace: str, start: str, end: str,
                            metric: Annotated[str, Field(pattern=r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")], pod: str | None = None,
                            step_seconds: Annotated[int, Field(ge=15, le=3600)] = 60) -> dict[str, Any]:
            """Read a named raw metric with mandatory service/namespace labels; max 100 series."""
            scope = scope_for(service, namespace, start, end)
            if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", metric):
                raise ValueError("metric must be a name, not a PromQL expression")
            if (scope.end - scope.start).total_seconds() / step_seconds > 1440:
                raise ValueError("Too many time points; increase step_seconds")
            query = metric + client.labels(service, namespace, pod)
            result = client.get("PROMETHEUS", "/api/v1/query_range", {"query": query, "start": scope.start.isoformat(),
                                "end": scope.end.isoformat(), "step": step_seconds, "limit": 100, "timeout": "10s"})
            if result.get("status") != "success":
                raise ValueError("Prometheus query did not succeed")
            return {"source": "prometheus", "scope": scope.model_dump(mode="json"), "query": query, "result": result}

    if env.get("TEMPO_URL"):
        @server.tool()
        def service_trace_detail(service: str, namespace: str, start: str, end: str,
                                 trace_id: Annotated[str, Field(pattern=r"^(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})$")]) -> dict[str, Any]:
            """Fetch a trace, returning only spans in the requested service/namespace/time scope."""
            scope = scope_for(service, namespace, start, end)
            if not re.fullmatch(r"(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})", trace_id):
                raise ValueError("Invalid trace ID")
            result = client.get("TEMPO", f"/api/v2/traces/{trace_id}", {})
            trace = result.get("trace", result)
            if not isinstance(trace, dict):
                raise ValueError("Invalid Tempo trace envelope")
            batches = trace.get("resourceSpans", trace.get("batches", []))
            spans = []
            truncated = False
            for batch in batches:
                attributes = {a.get("key"): a.get("value", {}).get("stringValue")
                              for a in batch.get("resource", {}).get("attributes", [])}
                if (attributes.get("service.name") != service
                        or attributes.get("k8s.namespace.name") != namespace):
                    continue
                for group in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
                    for span in group.get("spans", []):
                        try:
                            at = int(span["startTimeUnixNano"]) / 1_000_000_000
                        except (KeyError, TypeError, ValueError):
                            continue
                        if scope.start.timestamp() <= at <= scope.end.timestamp():
                            if len(spans) >= 200:
                                truncated = True
                                continue
                            spans.append(span)
            return {"source": "tempo", "scope": scope.model_dump(mode="json"), "trace_id": trace_id,
                    "spans": spans, "scope_filtered": True, "truncated": truncated,
                    "observation_status": "success" if spans else "no_data", "collected_at": utc_now().isoformat()}

        @server.tool()
        def service_traces(service: str, namespace: str, start: str, end: str,
                           limit: Annotated[int, Field(ge=1, le=50)] = 20) -> dict[str, Any]:
            """Search traces by resource.service.name and resource.k8s.namespace.name."""
            scope = scope_for(service, namespace, start, end)
            query = "{ resource.service.name = " + json.dumps(service) + " && resource.k8s.namespace.name = " + json.dumps(namespace) + " }"
            result = client.get("TEMPO", "/api/search", {"q": query, "start": int(scope.start.timestamp()),
                                                       "end": int(scope.end.timestamp()), "limit": limit})
            return {"source": "tempo", "scope": scope.model_dump(mode="json"), "query": query, "result": result}

    if env.get("KUBERNETES_URL"):
        @server.tool()
        def service_events(service: str, namespace: str, start: str, end: str,
                           pod: Annotated[str, Field(pattern=r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")]) -> dict[str, Any]:
            """Read retained events for an authorized current Pod; event retention may be incomplete."""
            scope = scope_for(service, namespace, start, end)
            if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace):
                raise ValueError("Invalid namespace")
            if not re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", pod):
                raise ValueError("Invalid Pod name")
            encoded_ns, encoded_pod = quote(namespace, safe=""), quote(pod, safe="")
            instance = client.get("KUBERNETES", f"/api/v1/namespaces/{encoded_ns}/pods/{encoded_pod}", {})
            if instance.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name") != service:
                raise PermissionError("Pod is outside the requested service")
            uid = instance.get("metadata", {}).get("uid", "")
            if not re.fullmatch(r"[A-Za-z0-9-]+", uid):
                raise ValueError("Missing Pod UID")
            result = client.get("KUBERNETES", f"/api/v1/namespaces/{encoded_ns}/events", {
                "fieldSelector": f"involvedObject.uid={uid}", "limit": 100})
            events = []
            for event in result.get("items", []):
                stamp = event.get("lastTimestamp") or event.get("eventTime") or event.get("metadata", {}).get("creationTimestamp")
                try:
                    at = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                    in_window = at.utcoffset() is not None and scope.start <= at <= scope.end
                except (TypeError, ValueError, AttributeError):
                    in_window = False
                if in_window:
                    events.append({k: event.get(k) for k in ["reason", "message", "type", "count", "eventTime", "firstTimestamp", "lastTimestamp"]})
            return {"source": "kubernetes", "scope": scope.model_dump(mode="json"), "pod": pod, "events": events,
                    "truncated": bool(result.get("metadata", {}).get("continue")),
                    "temporal_semantics": "retained_events_for_current_pod_uid", "collected_at": utc_now().isoformat()}

        @server.tool()
        def service_instances(service: str, namespace: str, start: str, end: str) -> dict[str, Any]:
            """Read CURRENT pod status; this snapshot cannot establish historical incident state."""
            scope = scope_for(service, namespace, start, end)
            if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace):
                raise ValueError("Invalid Kubernetes namespace")
            if not re.fullmatch(r"[A-Za-z0-9]([-_.A-Za-z0-9]*[A-Za-z0-9])?", service):
                raise ValueError("Invalid service label value")
            result = client.get("KUBERNETES", f"/api/v1/namespaces/{quote(namespace, safe='')}/pods",
                                {"labelSelector": "app.kubernetes.io/name=" + service, "limit": 100})
            pods = [{"name": item.get("metadata", {}).get("name"), "status": item.get("status", {})} for item in result.get("items", [])]
            return {"source": "kubernetes", "scope": scope.model_dump(mode="json"), "pods": pods,
                    "collected_at": utc_now().isoformat(), "temporal_semantics": "current_snapshot",
                    "truncated": bool(result.get("metadata", {}).get("continue"))}
    return server


if __name__ == "__main__":
    build_server().run(transport="stdio")
