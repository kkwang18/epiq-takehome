import argparse

from content_intake.stub.run import run


def add_stub_parser(subparsers) -> None:
    p = subparsers.add_parser("stub")
    p.add_argument("--port", type=int, default=8080)


def run_stub(args: argparse.Namespace) -> int:
    run(port=args.port)
    return 0
