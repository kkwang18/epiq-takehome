# src/content_intake/cli/query_cmd.py
import argparse
import json
import sys
from pathlib import Path

import httpx

from content_intake.common import config


def add_submit_parser(subparsers) -> None:
    p = subparsers.add_parser("submit")
    p.add_argument("--corpus", required=True)
    p.add_argument("--tenant", required=True)


def run_submit(args: argparse.Namespace) -> int:
    corpus_dir = str(Path(args.corpus).resolve())
    resp = httpx.post(f"{config.API_BASE_URL}/v1/runs", json={"corpus_dir": corpus_dir, "tenant": args.tenant})
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_status_parser(subparsers) -> None:
    p = subparsers.add_parser("status")
    p.add_argument("--run", required=True, dest="run_id")


def run_status(args: argparse.Namespace) -> int:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{args.run_id}/status")
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_item_parser(subparsers) -> None:
    p = subparsers.add_parser("item")
    p.add_argument("--tenant", required=True)
    p.add_argument("--id", required=True, dest="item_id")


def run_item(args: argparse.Namespace) -> int:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/items/{args.item_id}", params={"tenant": args.tenant})
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_items_parser(subparsers) -> None:
    p = subparsers.add_parser("items")
    p.add_argument("--tenant", required=True)
    p.add_argument("--run", required=True, dest="run_id")
    p.add_argument("--state", default=None)


def run_items(args: argparse.Namespace) -> int:
    params = {"tenant": args.tenant}
    if args.state:
        params["state"] = args.state
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{args.run_id}/items", params=params)
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0
