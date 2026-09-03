import hashlib
import json
from typing import Any


def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_of_canonical(obj: Any) -> str:
    return sha256_hex(canonical_dumps(obj).encode("utf-8"))
