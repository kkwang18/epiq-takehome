import base64
import json

import httpx
import pytest

from content_intake.pipeline.stub_client import call_annotate


def test_success_response_parsed():
    def handler(request):
        assert json.loads(request.content) == {"content_b64": base64.b64encode(b"x").decode()}
        return httpx.Response(200, json={"sha256": "abc"})

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.status_code == 200
    assert result.body == {"sha256": "abc"}
    assert result.error is None


def test_server_error_response_parsed():
    transport = httpx.MockTransport(lambda r: httpx.Response(500, json={"error": {"code": "server_error"}}))
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.status_code == 500
    assert result.body == {"error": {"code": "server_error"}}


def test_timeout_is_reported_as_error():
    def handler(request):
        raise httpx.TimeoutException("timed out")

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.error == "timeout"
    assert result.status_code is None


def test_connection_error_is_reported_as_error():
    def handler(request):
        raise httpx.ConnectError("refused")

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.error == "connection_error"
