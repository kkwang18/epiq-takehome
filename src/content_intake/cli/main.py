import argparse
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="intake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("version")
    args = parser.parse_args(argv)
    if args.command == "version":
        print("content-intake-pipeline 0.1.0")
        return 0
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
