# src/content_intake/stub/app.py
import base64
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from content_intake.stub.state import StubState

_ALLOWED_ANNOTATE_FIELDS = {"content_b64"}


def create_app(state: StubState) -> FastAPI:
    app = FastAPI()

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/v1/stats")
    def stats():
        return state.stats()

    @app.post("/v1/reset")
    async def reset(request: Request):
        try:
            patch = await request.json()
        except Exception:
            patch = None
        state.reset(patch or None)
        return {"ok": True}

    @app.post("/v1/annotate")
    async def annotate(request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})
        if not isinstance(body, dict) or set(body.keys()) != _ALLOWED_ANNOTATE_FIELDS:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})
        try:
            content = base64.b64decode(body["content_b64"], validate=True)
        except Exception:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})

        admitted = state.enter_call()
        try:
            delay_ms = state.next_latency_ms()
            time.sleep(delay_ms / 1000.0)
            should_fail = state.bill_and_check_failure()
            if not admitted:
                state.record_over_capacity()
                return JSONResponse(status_code=429, content={"error": {"code": "over_capacity"}})
            if should_fail:
                state.record_server_error()
                return JSONResponse(status_code=state.failure_status, content={"error": {"code": "server_error"}})
            return state.annotate(content)
        finally:
            if admitted:
                state.exit_call()

    return app
