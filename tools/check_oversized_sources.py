#!/usr/bin/env python3
"""Report production source files that exceed the repository line threshold."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


DEFAULT_THRESHOLD = 1500
SOURCE_SUFFIXES = {
    ".css",
    ".html",
    ".js",
    ".jsx",
    ".py",
    ".rs",
    ".sh",
    ".sql",
    ".ts",
    ".tsx",
}
EXCLUDED_DIRS = {
    ".git",
    ".codex-tmp",
    ".tmp",
    "__pycache__",
    "coverage",
    "dist",
    "build",
    "logs",
    "node_modules",
    "target",
    "tmp",
}
EXCLUDED_FILES = {
    "Cargo.lock",
    "package-lock.json",
    "schema_contract.rs",
}
GENERATED_MARKERS = {
    "generated",
    "docs/microsoft/cache",
}
TEST_DIRS = {
    "__tests__",
    "test",
    "tests",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report tracked production source files above the line-count threshold."
        )
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD,
        help=f"maximum allowed lines before a warning is reported (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--fail",
        action="store_true",
        help="exit with status 1 when oversized files are found",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="include files under test directories in the report",
    )
    return parser.parse_args()


def repo_root() -> Path:
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if output:
            return Path(output)
    except (OSError, subprocess.CalledProcessError):
        pass
    return Path.cwd()


def tracked_files(root: Path) -> list[Path]:
    try:
        output = subprocess.check_output(
            ["git", "-C", str(root), "ls-files"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return walked_files(root)
    return [root / line for line in output.splitlines() if line]


def walked_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in EXCLUDED_DIRS]
        current = Path(dirpath)
        files.extend(current / name for name in filenames)
    return files


def is_generated(relative_posix: str) -> bool:
    return any(
        relative_posix == marker or relative_posix.startswith(f"{marker}/")
        for marker in GENERATED_MARKERS
    )


def is_test_path(path: Path) -> bool:
    return any(part in TEST_DIRS for part in path.parts) or path.name in {
        "tests.rs",
        "test.rs",
    }


def is_source_file(root: Path, path: Path, include_tests: bool) -> bool:
    if path.name in EXCLUDED_FILES:
        return False
    if path.suffix not in SOURCE_SUFFIXES:
        return False
    relative = path.relative_to(root)
    if any(part in EXCLUDED_DIRS for part in relative.parts):
        return False
    relative_posix = relative.as_posix()
    if is_generated(relative_posix):
        return False
    if not include_tests and is_test_path(relative):
        return False
    return True


def count_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def main() -> int:
    args = parse_args()
    if args.threshold < 1:
        print("--threshold must be positive", file=sys.stderr)
        return 2

    root = repo_root()
    offenders: list[tuple[int, str]] = []
    checked = 0
    for path in tracked_files(root):
        if not path.is_file() or not is_source_file(root, path, args.include_tests):
            continue
        checked += 1
        lines = count_lines(path)
        if lines > args.threshold:
            offenders.append((lines, path.relative_to(root).as_posix()))

    offenders.sort(key=lambda item: (-item[0], item[1]))
    print(f"Checked {checked} production source files; threshold is {args.threshold} lines.")
    if not offenders:
        print("No oversized production source files found.")
        return 0

    print("Oversized production source files:")
    for lines, name in offenders:
        print(f"{lines:>6}  {name}")
    if args.fail:
        return 1
    print("Warning only. Pass --fail to make oversized files fail the check.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
