# src/content_intake/common/config.py
import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


IN_FLIGHT_CAPACITY = _int("INTAKE_IN_FLIGHT_CAPACITY", 2)
LEASE_SECONDS = _int("INTAKE_LEASE_SECONDS", 5)
LEASE_RENEW_INTERVAL = _int("INTAKE_LEASE_RENEW_INTERVAL", 2)
HTTP_TIMEOUT_SECONDS = _float("INTAKE_HTTP_TIMEOUT_SECONDS", 2.0)
MAX_ATTEMPTS = _int("INTAKE_MAX_ATTEMPTS", 5)
BACKOFF_BASE = _float("INTAKE_BACKOFF_BASE", 0.2)
BACKOFF_CAP = _float("INTAKE_BACKOFF_CAP", 2.0)
BACKOFF_JITTER = _float("INTAKE_BACKOFF_JITTER", 0.2)
STUB_PORT = _int("INTAKE_STUB_PORT", 8080)
API_PORT = _int("INTAKE_API_PORT", 8090)
STUB_BASE_URL = os.environ.get("INTAKE_STUB_BASE_URL", f"http://localhost:{STUB_PORT}")
API_BASE_URL = os.environ.get("INTAKE_API_BASE_URL", f"http://localhost:{API_PORT}")
