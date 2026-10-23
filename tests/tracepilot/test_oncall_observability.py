from datetime import datetime
from urllib.parse import parse_qs, urlsplit

import anyio
import pytest
import responses

from tracepilot.config import MCPServerConfig, ScopeBinding, ToolPolicy
from tracepilot.mcp_gateway import MCPGateway
from tracepilot.models import Candidate
from tracepilot.observability_server import build_server
from tracepilot.scenarios import demo_scope


@pytest.mark.parametrize("signal,fragment", [("latency_p95", "histogram_quantile"), ("error_rate", 'status=~"5.."'), ("request_rate", "sum(rate")])
def test_metric_templates_are_scoped_and_include_rate_lookback(signal, fragment):
    server = build_server({"PROMETHEUS_URL": "https://obs.test"})
    with responses.RequestsMock() as http:
        http.get("https://obs.test/api/v1/query_range", json={"status": "success", "data": {"result": []}})
        result = anyio.run(server.call_tool, "service_metric_summary", {**demo_scope().model_dump(mode="json"), "signal": signal})
        params = parse_qs(urlsplit(http.calls[0].request.url).query)
        assert fragment in params["query"][0]
        assert 'service="checkout"' in params["query"][0] and 'namespace="demo"' in params["query"][0]
        assert datetime.fromisoformat(params["start"][0]) > demo_scope().start
        assert "no_data" in str(result)


def test_trace_details_filter_other_services_namespaces_and_times():
    server = build_server({"TEMPO_URL": "https://obs.test"})
    at = int(demo_scope().start.timestamp() * 1_000_000_000)

    def batch(service, namespace, name, time):
        return {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}},
            {"key": "k8s.namespace.name", "value": {"stringValue": namespace}}]},
            "scopeSpans": [{"spans": [{"name": name, "startTimeUnixNano": str(time)}]}]}

    payload = {"trace": {"resourceSpans": [batch("checkout", "demo", "allowed-span", at + 10**9),
        batch("billing", "demo", "private-service", at + 10**9), batch("checkout", "private", "private-namespace", at + 10**9),
        batch("checkout", "demo", "outside-time", at - 10**9)]}}
    with responses.RequestsMock() as http:
        http.get("https://obs.test/api/v2/traces/0123456789abcdef", json=payload)
        result = str(anyio.run(server.call_tool, "service_trace_detail", {
            **demo_scope().model_dump(mode="json"), "trace_id": "0123456789abcdef"}))
        assert "allowed-span" in result
        assert all(name not in result for name in ["private-service", "private-namespace", "outside-time"])


def test_events_require_pod_service_membership_before_reading_events():
    server = build_server({"KUBERNETES_URL": "https://obs.test"})
    with responses.RequestsMock() as http:
        http.get("https://obs.test/api/v1/namespaces/demo/pods/checkout-a", json={"metadata": {
            "uid": "pod-uid", "labels": {"app.kubernetes.io/name": "billing"}}})
        with pytest.raises(Exception):
            anyio.run(server.call_tool, "service_events", {**demo_scope().model_dump(mode="json"), "pod": "checkout-a"})
        assert len(http.calls) == 1


def test_explicit_dependency_scope_does_not_authorize_other_services():
    args = {**demo_scope().model_dump(mode="json"), "service": "inventory"}
    ScopeBinding().validate_arguments(args, demo_scope(), frozenset({"checkout", "inventory"}))
    args["service"] = "billing"
    with pytest.raises(PermissionError):
        ScopeBinding().validate_arguments(args, demo_scope(), frozenset({"checkout", "inventory"}))


def test_local_preflight_rejects_scope_before_a_candidate_is_offered():
    config = MCPServerConfig(command="unused", tools={"logs": ToolPolicy(read_only=True)})
    gateway = MCPGateway({"obs": config}, demo_scope())
    gateway._routes = {"obs__logs": ("obs", "logs")}
    candidate = Candidate(id="unsafe", tool="obs__logs", arguments={**demo_scope().model_dump(mode="json"), "service": "billing"},
                          purpose="Invalid service", direction="logs")
    with pytest.raises(PermissionError):
        gateway.validate_candidate(candidate)
