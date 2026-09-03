import argparse
import json
import sys
from pathlib import Path

from content_intake.generator.generate import generate_corpus
from content_intake.generator.verify import verify_corpus


def add_corpus_parser(subparsers) -> None:
    p = subparsers.add_parser("corpus")
    p.add_argument("--seed", type=int)
    p.add_argument("--size", type=int)
    p.add_argument("--tenant", type=str)
    p.add_argument("--out", type=str, required=True)
    p.add_argument("--force", action="store_true")
    p.add_argument("--verify", action="store_true")


def run_corpus(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    if args.verify:
        mismatches = verify_corpus(out_dir)
        if mismatches:
            for m in mismatches:
                print(m, file=sys.stderr)
            return 1
        print(json.dumps({"verified": True, "out": str(out_dir)}))
        return 0

    if args.seed is None or args.size is None or args.tenant is None:
        print("corpus generation requires --seed, --size, and --tenant", file=sys.stderr)
        return 1
    if not (50 <= args.size <= 500):
        print("--size must be between 50 and 500 inclusive", file=sys.stderr)
        return 1
    try:
        manifest = generate_corpus(
            seed=args.seed, size=args.size, tenant=args.tenant, out_dir=out_dir, force=args.force
        )
    except FileExistsError as e:
        print(str(e), file=sys.stderr)
        return 1
    print(json.dumps({"corpus_id": manifest["corpus_id"], "out": str(out_dir), "items": manifest["totals"]["items"]}))
    return 0
