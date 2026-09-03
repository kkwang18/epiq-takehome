import hashlib
from content_intake.common.canonical_json import canonical_dumps, sha256_hex, sha256_of_canonical


def test_canonical_dumps_sorts_keys_and_is_compact():
    assert canonical_dumps({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_dumps_deterministic_across_calls():
    obj = {"z": [3, 2, 1], "a": {"y": 1, "x": 2}}
    assert canonical_dumps(obj) == canonical_dumps(obj)


def test_sha256_hex_matches_stdlib():
    data = b"hello world"
    assert sha256_hex(data) == hashlib.sha256(data).hexdigest()


def test_sha256_of_canonical_changes_with_content():
    a = sha256_of_canonical({"a": 1})
    b = sha256_of_canonical({"a": 2})
    assert a != b
