# src/content_intake/scenario/profile.py
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class ScenarioProfile:
    tenant_a: str
    seed_a: int
    size_a: int
    tenant_b: str
    seed_b: int
    size_b: int
    corpus_a_dir: Path
    corpus_b_dir: Path
    workers: int = 4
    poll_interval: float = 0.5
    failure_every_n: int = 7
    max_attempts: int = 5


DEFAULT_PROFILE = ScenarioProfile(
    tenant_a="tenant-a", seed_a=1, size_a=500,
    tenant_b="tenant-b", seed_b=2, size_b=400,
    corpus_a_dir=REPO_ROOT / ".scenario_corpora" / "tenant-a",
    corpus_b_dir=REPO_ROOT / ".scenario_corpora" / "tenant-b",
)
