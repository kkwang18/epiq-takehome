import io
import json
import shutil
from pathlib import Path
from random import Random

from PIL import Image

from content_intake.common.canonical_json import sha256_hex, sha256_of_canonical

EXTENSIONS = ["txt", "json", "csv", "png"]
WORDS = [
    "delta", "harbor", "quiet", "ledger", "signal", "orbit", "cinder", "maple",
    "vault", "ridge", "amber", "north", "spindle", "cobalt", "lantern", "wren",
]


def _item_rng(seed: int, index: int) -> Random:
    return Random(f"{seed}:{index}")


def _make_txt(rng: Random) -> bytes:
    n_words = rng.randint(20, 60)
    words = [rng.choice(WORDS) for _ in range(n_words)]
    return (" ".join(words) + "\n").encode("utf-8")


def _make_json(rng: Random) -> bytes:
    obj = {
        "id": rng.randint(1, 1_000_000),
        "tags": [rng.choice(WORDS) for _ in range(rng.randint(1, 5))],
        "value": round(rng.uniform(0, 1000), 3),
    }
    return (json.dumps(obj) + "\n").encode("utf-8")


def _make_csv(rng: Random) -> bytes:
    n_rows = rng.randint(3, 10)
    lines = ["col_a,col_b,col_c"]
    for _ in range(n_rows):
        lines.append(f"{rng.randint(0,100)},{rng.choice(WORDS)},{round(rng.uniform(0,1),3)}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _make_png(rng: Random) -> bytes:
    size = 8
    img = Image.new("RGB", (size, size))
    pixels = [
        (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
        for _ in range(size * size)
    ]
    img.putdata(pixels)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


_MAKERS = {"txt": _make_txt, "json": _make_json, "csv": _make_csv, "png": _make_png}


def generate_corpus(seed: int, size: int, tenant: str, out_dir: Path, force: bool = False) -> dict:
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        if not force:
            raise FileExistsError(f"{out_dir} is not empty; pass force=True to overwrite")
        shutil.rmtree(out_dir)
    files_dir = out_dir / "files"
    files_dir.mkdir(parents=True)
    (out_dir / "files" / "edge").mkdir()

    n_dup = round(size / 10)
    n_edge = 2
    n_original = size - n_dup - n_edge

    originals: list[dict] = []
    for i in range(n_original):
        ext = EXTENSIONS[i % len(EXTENSIONS)]
        rng = _item_rng(seed, i)
        data = _MAKERS[ext](rng)
        path = f"orig_{i}.{ext}"
        (files_dir / path).write_bytes(data)
        originals.append({"path": path, "extension": ext, "bytes": data})

    duplicates: list[dict] = []
    for i in range(n_dup):
        rng = _item_rng(seed, n_original + i)
        source = rng.choice(originals)
        path = f"dup_{i}.{source['extension']}"
        (files_dir / path).write_bytes(source["bytes"])
        duplicates.append({"path": path, "extension": source["extension"], "bytes": source["bytes"], "duplicate_of": source["path"]})

    empty_path = "edge/empty.txt"
    (files_dir / empty_path).write_bytes(b"")

    malformed_rng = _item_rng(seed, n_original + n_dup + 1)
    malformed_bytes = _make_png(malformed_rng)
    malformed_path = "edge/malformed.json"
    (files_dir / malformed_path).write_bytes(malformed_bytes)

    entries = []
    order = 0
    for o in originals:
        entries.append({
            "order": order, "path": o["path"], "extension": o["extension"], "bytes": len(o["bytes"]),
            "sha256": sha256_hex(o["bytes"]), "role": "original", "duplicate_of": None,
            "edge_case": None, "expected_outcome": "success", "expects_annotation": True,
        })
        order += 1
    for d in duplicates:
        entries.append({
            "order": order, "path": d["path"], "extension": d["extension"], "bytes": len(d["bytes"]),
            "sha256": sha256_hex(d["bytes"]), "role": "duplicate", "duplicate_of": d["duplicate_of"],
            "edge_case": None, "expected_outcome": "success", "expects_annotation": True,
        })
        order += 1
    entries.append({
        "order": order, "path": empty_path, "extension": "txt", "bytes": 0,
        "sha256": sha256_hex(b""), "role": "edge_case", "duplicate_of": None,
        "edge_case": "empty_content", "expected_outcome": "empty_content", "expects_annotation": False,
    })
    order += 1
    entries.append({
        "order": order, "path": malformed_path, "extension": "json", "bytes": len(malformed_bytes),
        "sha256": sha256_hex(malformed_bytes), "role": "edge_case", "duplicate_of": None,
        "edge_case": "decode_failed", "expected_outcome": "decode_failed", "expects_annotation": False,
    })

    manifest = {
        "corpus_id": f"{tenant}-{seed}-{size}",
        "seed": seed,
        "size": size,
        "tenant": tenant,
        "totals": {"items": len(entries), "duplicates": n_dup, "edge_cases": n_edge},
        "edge_cases": {"empty_content": empty_path, "decode_failed": malformed_path},
        "files": entries,
    }
    manifest["digest"] = sha256_of_canonical(manifest)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest
