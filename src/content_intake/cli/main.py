import argparse
import sys

from content_intake.cli.corpus_cmd import add_corpus_parser, run_corpus
from content_intake.cli.stub_cmd import add_stub_parser, run_stub


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="intake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("version")
    add_corpus_parser(subparsers)
    add_stub_parser(subparsers)
    args = parser.parse_args(argv)

    if args.command == "version":
        print("content-intake-pipeline 0.1.0")
        return 0
    if args.command == "corpus":
        return run_corpus(args)
    if args.command == "stub":
        return run_stub(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
