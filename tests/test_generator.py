import json
from pathlib import Path

from content_intake.generator.generate import generate_corpus


def test_determinism_same_seed_size_tenant_byte_identical(tmp_path):
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    m1 = generate_corpus(seed=42, size=60, tenant="tenant-a", out_dir=out1)
    m2 = generate_corpus(seed=42, size=60, tenant="tenant-a", out_dir=out2)
    for entry in m1["files"]:
        p1 = out1 / "files" / entry["path"]
        p2 = out2 / "files" / entry["path"]
        assert p1.read_bytes() == p2.read_bytes()
    assert m1["digest"] == m2["digest"]


def test_file_bytes_depend_only_on_seed_and_size_not_tenant(tmp_path):
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    m1 = generate_corpus(seed=7, size=55, tenant="tenant-a", out_dir=out1)
    m2 = generate_corpus(seed=7, size=55, tenant="tenant-b", out_dir=out2)
    for e1, e2 in zip(m1["files"], m2["files"]):
        assert (out1 / "files" / e1["path"]).read_bytes() == (out2 / "files" / e2["path"]).read_bytes()
    assert m1["digest"] != m2["digest"]  # tenant enters the manifest


def test_item_count_matches_size(tmp_path):
    m = generate_corpus(seed=1, size=50, tenant="t", out_dir=tmp_path / "c")
    assert len(m["files"]) == 50
    assert m["totals"]["items"] == 50


def test_mixed_types_present(tmp_path):
    m = generate_corpus(seed=1, size=100, tenant="t", out_dir=tmp_path / "c")
    exts = {e["extension"] for e in m["files"]}
    assert {"txt", "json", "csv", "png"} <= exts


def test_duplicate_rate_within_tolerance(tmp_path):
    size = 100
    m = generate_corpus(seed=1, size=size, tenant="t", out_dir=tmp_path / "c")
    n_dup = sum(1 for e in m["files"] if e["role"] == "duplicate")
    assert abs(n_dup - size / 10) <= 2


def test_duplicates_are_byte_identical_and_same_extension(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=100, tenant="t", out_dir=out)
    by_path = {e["path"]: e for e in m["files"]}
    for e in m["files"]:
        if e["role"] != "duplicate":
            continue
        original = by_path[e["duplicate_of"]]
        assert original["extension"] == e["extension"]
        assert (out / "files" / e["path"]).read_bytes() == (out / "files" / original["path"]).read_bytes()


def test_empty_content_edge_case(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=60, tenant="t", out_dir=out)
    path = m["edge_cases"]["empty_content"]
    entry = next(e for e in m["files"] if e["path"] == path)
    assert entry["edge_case"] == "empty_content"
    assert entry["expected_outcome"] == "empty_content"
    assert entry["expects_annotation"] is False
    assert (out / "files" / path).read_bytes() == b""


def test_decode_failed_edge_case_is_png_bytes_named_json(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=60, tenant="t", out_dir=out)
    path = m["edge_cases"]["decode_failed"]
    entry = next(e for e in m["files"] if e["path"] == path)
    assert entry["edge_case"] == "decode_failed"
    assert entry["expected_outcome"] == "decode_failed"
    assert entry["expects_annotation"] is False
    assert entry["extension"] == "json"
    data = (out / "files" / path).read_bytes()
    assert data.startswith(b"\x89PNG\r\n\x1a\n")
    import json as _json
    import pytest as _pytest
    with _pytest.raises(Exception):
        _json.loads(data)


def test_manifest_written_to_disk_next_to_files(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    assert (out / "manifest.json").exists()
    assert (out / "files").is_dir()
    on_disk = json.loads((out / "manifest.json").read_text())
    assert on_disk["seed"] == 1


def test_force_overwrites_nonempty_directory(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    generate_corpus(seed=2, size=50, tenant="t", out_dir=out, force=True)
    m = json.loads((out / "manifest.json").read_text())
    assert m["seed"] == 2


def test_refuses_nonempty_directory_without_force(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    import pytest
    with pytest.raises(FileExistsError):
        generate_corpus(seed=2, size=50, tenant="t", out_dir=out, force=False)
