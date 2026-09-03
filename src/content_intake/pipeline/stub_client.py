import base64

import httpx


class StubCallResult:
    def __init__(self, status_code: int | None, body: dict | None, error: str | None):
        self.status_code = status_code
        self.body = body
        self.error = error


def call_annotate(client: httpx.Client, base_url: str, content: bytes, timeout: float) -> StubCallResult:
    payload = {"content_b64": base64.b64encode(content).decode("ascii")}
    try:
        resp = client.post(f"{base_url}/v1/annotate", json=payload, timeout=timeout)
    except httpx.TimeoutException:
        return StubCallResult(None, None, "timeout")
    except httpx.TransportError:
        return StubCallResult(None, None, "connection_error")
    try:
        body = resp.json()
    except ValueError:
        body = None
    return StubCallResult(resp.status_code, body, None)
