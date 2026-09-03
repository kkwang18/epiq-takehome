import os

import uvicorn

from content_intake.stub.app import create_app
from content_intake.stub.state import StubState


def _env(name: str, default, cast):
    val = os.environ.get(f"STUB_{name.upper()}")
    return cast(val) if val is not None else default


def build_state_from_env() -> StubState:
    return StubState(
        latency_mode=_env("latency_mode", "fixed", str),
        latency_ms=_env("latency_ms", 150, int),
        latency_jitter_min_ms=_env("latency_jitter_min_ms", 50, int),
        latency_jitter_max_ms=_env("latency_jitter_max_ms", 250, int),
        latency_seed=_env("latency_seed", 20260803, int),
        failure_every_n=_env("failure_every_n", 7, int),
        failure_status=_env("failure_status", 500, int),
        in_flight_capacity=_env("in_flight_capacity", 2, int),
    )


def run(port: int = 8080) -> None:
    state = build_state_from_env()
    app = create_app(state)
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1, log_level="info")


if __name__ == "__main__":
    run()
