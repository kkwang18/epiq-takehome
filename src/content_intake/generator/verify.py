import json
import tempfile
from pathlib import Path

from content_intake.generator.generate import generate_corpus


def verify_corpus(out_dir: Path) -> list[str]:
    out_dir = Path(out_dir)
    manifest = json.loads((out_dir / "manifest.json").read_text())
    mismatches: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        fresh_dir = Path(tmp) / "fresh"
        fresh_manifest = generate_corpus(
            seed=manifest["seed"], size=manifest["size"], tenant=manifest["tenant"], out_dir=fresh_dir
        )
        if fresh_manifest["digest"] != manifest["digest"]:
            mismatches.append(f"manifest digest differs: on-disk={manifest['digest']} fresh={fresh_manifest['digest']}")
        for entry in fresh_manifest["files"]:
            on_disk_path = out_dir / "files" / entry["path"]
            fresh_path = fresh_dir / "files" / entry["path"]
            if not on_disk_path.exists():
                mismatches.append(f"missing on disk: {entry['path']}")
                continue
            if on_disk_path.read_bytes() != fresh_path.read_bytes():
                mismatches.append(f"byte mismatch: {entry['path']}")
    return mismatches
