import argparse
import sys

from content_intake.cli.corpus_cmd import add_corpus_parser, run_corpus
from content_intake.cli.stub_cmd import add_stub_parser, run_stub
from content_intake.cli.query_cmd import (
    add_submit_parser, run_submit, add_status_parser, run_status,
    add_item_parser, run_item, add_items_parser, run_items,
)
from content_intake.cli.lifecycle_cmd import (
    add_up_parser, run_up, add_down_parser, run_down, add_reset_parser, run_reset,
)
from content_intake.cli.kill_worker_cmd import add_kill_worker_parser, run_kill_worker
from content_intake.cli.scenario_cmd import add_scenario_parser, run_scenario


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="intake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("version")
    add_corpus_parser(subparsers)
    add_stub_parser(subparsers)
    add_submit_parser(subparsers)
    add_status_parser(subparsers)
    add_item_parser(subparsers)
    add_items_parser(subparsers)
    add_up_parser(subparsers)
    add_down_parser(subparsers)
    add_reset_parser(subparsers)
    add_kill_worker_parser(subparsers)
    add_scenario_parser(subparsers)
    args = parser.parse_args(argv)

    if args.command == "version":
        print("content-intake-pipeline 0.1.0")
        return 0
    if args.command == "corpus":
        return run_corpus(args)
    if args.command == "stub":
        return run_stub(args)
    if args.command == "submit":
        return run_submit(args)
    if args.command == "status":
        return run_status(args)
    if args.command == "item":
        return run_item(args)
    if args.command == "items":
        return run_items(args)
    if args.command == "up":
        return run_up(args)
    if args.command == "down":
        return run_down(args)
    if args.command == "reset":
        return run_reset(args)
    if args.command == "kill-worker":
        return run_kill_worker(args)
    if args.command == "scenario":
        return run_scenario(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
