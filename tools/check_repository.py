#!/usr/bin/env python3
"""Run lightweight repository maintenance checks."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run portable repository checks for local review and CI."
    )
    parser.add_argument(
        "--fail-oversized",
        action="store_true",
        help="fail when production source files exceed the line-count threshold",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="include test files in the oversized-source scan",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    command = [sys.executable, str(root / "tools" / "check_oversized_sources.py")]
    if args.fail_oversized:
        command.append("--fail")
    if args.include_tests:
        command.append("--include-tests")
    return subprocess.call(command, cwd=root)


if __name__ == "__main__":
    raise SystemExit(main())
