import json
from pathlib import Path

from content_intake.generator.generate import generate_corpus
from content_intake.generator.verify import verify_corpus


def test_verify_passes_on_untouched_corpus(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    assert verify_corpus(out) == []


def test_verify_detects_modified_file(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    target = out / "files" / m["files"][0]["path"]
    target.write_bytes(target.read_bytes() + b"tampered")
    mismatches = verify_corpus(out)
    assert mismatches != []
    assert any(m["files"][0]["path"] in msg for msg in mismatches)


def test_verify_detects_missing_file(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    (out / "files" / m["files"][0]["path"]).unlink()
    mismatches = verify_corpus(out)
    assert mismatches != []
