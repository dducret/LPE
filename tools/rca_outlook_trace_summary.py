#!/usr/bin/env python3
"""Summarize one captured Outlook MAPI run.

The server RR traces prove transport-level success/failure, while the journal
log carries the richer bootstrap diagnostics. This tool keeps the two views
side-by-side so a new Outlook run can be triaged without hand-scanning logs.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any


HEX_TAG_RE = re.compile(r"0x[0-9a-fA-F]{8}")
RUN_STAMP_RE = re.compile(r"(\d{12})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "trace_dir",
        type=Path,
        help="logs/outlook-traces/<run> directory, or logs/outlook-traces with --all",
    )
    parser.add_argument(
        "--log",
        type=Path,
        help="matching LPE_last_*.log journal export",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="summarize every child run directory and pair logs/LPE_last_<run>.log when present",
    )
    parser.add_argument(
        "--logs-root",
        type=Path,
        default=Path("logs"),
        help="directory containing LPE_last_*.log files for --all",
    )
    parser.add_argument(
        "--current-build",
        help="current deployed git commit prefix to highlight current-build failures in --all",
    )
    return parser.parse_args()


def load_json_line(line: str) -> dict[str, Any] | None:
    start = line.find("{")
    if start < 0:
        return None
    try:
        return json.loads(line[start:])
    except json.JSONDecodeError:
        return None


def summarize_rr(trace_dir: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "files": 0,
        "events": 0,
        "response_codes": Counter(),
        "nonzero_response_codes": Counter(),
        "http_statuses": Counter(),
        "sessions": defaultdict(lambda: deque(maxlen=12)),
        "parse_errors": Counter(),
        "request_sequences": Counter(),
    }
    for path in trace_jsonl_paths(trace_dir):
        summary["files"] += 1
        with path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                event = load_json_line(raw)
                if not event:
                    continue
                summary["events"] += 1
                metadata = event.get("metadata") or {}
                session_id = event.get("session_id") or "unknown"
                if "response_status" in event:
                    summary["http_statuses"][str(event["response_status"])] += 1
                response_code = metadata.get("mapi_response_code")
                if response_code not in (None, ""):
                    summary["response_codes"][str(response_code)] += 1
                    if str(response_code) != "0":
                        summary["nonzero_response_codes"][str(response_code)] += 1
                parse_error = metadata.get("request_rop_parse_error") or metadata.get(
                    "response_rop_parse_error"
                )
                if parse_error:
                    summary["parse_errors"][str(parse_error)] += 1
                if event.get("direction") == "inbound" and event.get("phase") == "Execute":
                    names = metadata.get("request_rop_names") or ""
                    if names:
                        summary["request_sequences"][names] += 1
                        summary["sessions"][session_id].append(names)
    return summary


def trace_jsonl_paths(trace_dir: Path) -> list[Path]:
    rr_paths = sorted(trace_dir.glob("*.rr.jsonl"))
    if rr_paths:
        return rr_paths
    return sorted(
        path
        for path in trace_dir.glob("*.jsonl")
        if not path.name.endswith(".replay.jsonl")
    )


def summarize_log(log_path: Path | None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "lines": 0,
        "execute_events": 0,
        "last_execute": None,
        "build": {},
        "stall_warnings": Counter(),
        "startup_missing_gates": Counter(),
        "sequence_counts": Counter(),
        "visible_release_without_query_rows": 0,
        "raw_visible_release_marker_lines": 0,
        "raw_umolk_placeholder": 0,
        "stale_default_view_states": Counter(),
        "visible_release_contexts": set(),
        "stale_default_view_contexts": set(),
        "unknown_getprops_tags": Counter(),
        "zero_default_tags": Counter(),
    }
    if not log_path:
        return summary
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            summary["lines"] += 1
            if "3c786d6c2f3e" in raw:
                summary["raw_umolk_placeholder"] += 1
            if "visible_inbox_release_without_query_rows=true" in raw:
                summary["raw_visible_release_marker_lines"] += 1
            event = load_json_line(raw)
            if not event:
                continue
            fields = event.get("fields") or {}
            message = fields.get("message") or ""
            if not summary["build"]:
                build = {
                    key: fields.get(key)
                    for key in (
                        "package_version",
                        "git_commit",
                        "git_commit_full",
                        "git_commit_time",
                        "git_dirty",
                    )
                    if fields.get(key) not in (None, "")
                }
                if build:
                    summary["build"] = build
            if message == "rca debug mapi execute rops":
                summary["execute_events"] += 1
                signature = fields.get("request_rop_sequence_signature") or fields.get(
                    "request_rop_names"
                )
                if signature:
                    summary["sequence_counts"][str(signature)] += 1
                missing_gate = fields.get("outlook_startup_first_missing_gate")
                if missing_gate:
                    summary["startup_missing_gates"][str(missing_gate)] += 1
                stall = fields.get("outlook_bootstrap_stall_name")
                if stall and stall != "none":
                    summary["stall_warnings"][str(stall)] += 1
                summary["last_execute"] = {
                    "request_id": fields.get("mapi_request_id"),
                    "signature": signature,
                    "missing_gate": missing_gate,
                    "stall": stall,
                    "normal_query_rows": fields.get(
                        "normal_inbox_contents_query_rows_observed"
                    ),
                }
                inspect_view_trace(summary, str(fields.get("outlook_view_trace_events") or ""))
            elif message == "rca debug mapi outlook surface getprops contract":
                contract = str(fields.get("getprops_contract") or "")
                inspect_contract(summary, contract)
    summary["visible_release_without_query_rows"] = len(summary["visible_release_contexts"])
    if summary["visible_release_without_query_rows"] == 0:
        summary["visible_release_without_query_rows"] = summary[
            "raw_visible_release_marker_lines"
        ]
    return summary


def inspect_view_trace(summary: dict[str, Any], trace_events: str) -> None:
    if not trace_events:
        return
    for segment in trace_events.split(">"):
        if not segment:
            continue
        if segment.startswith("default_view_folder_open:"):
            role = first_field(segment, "role")
            owner_role = first_field(segment, "owner_role")
            if role and owner_role and role != owner_role:
                if segment not in summary["stale_default_view_contexts"]:
                    summary["stale_default_view_contexts"].add(segment)
                    summary["stale_default_view_states"][f"{role}->{owner_role}"] += 1
        if "visible_inbox_release_without_query_rows=true" in segment:
            summary["visible_release_contexts"].add(segment)
        inspect_contract(summary, segment)


def first_field(text: str, key: str) -> str | None:
    prefix = f"{key}="
    for part in text.split(";"):
        part = part.strip()
        if part.startswith(prefix):
            return part[len(prefix) :].split(">", 1)[0]
    return None


def inspect_contract(summary: dict[str, Any], contract: str) -> None:
    if not contract:
        return
    if "names=unknown" in contract:
        for tag in tags_after(contract, "tags="):
            summary["unknown_getprops_tags"][tag] += 1
    for key in ("zero_default_tags=", "zero_defaults="):
        for tag in tags_after(contract, key):
            summary["zero_default_tags"][tag] += 1


def tags_after(text: str, key: str) -> list[str]:
    index = text.find(key)
    if index < 0:
        return []
    value = text[index + len(key) :].split(";", 1)[0].split(")", 1)[0]
    return HEX_TAG_RE.findall(value)


def print_counter(title: str, counter: Counter[str], limit: int = 12) -> None:
    print(title)
    if not counter:
        print("  none")
        return
    for key, count in counter.most_common(limit):
        print(f"  {key}: {count}")


def print_single_summary(
    trace_dir: Path, log_path: Path | None
) -> tuple[dict[str, Any], dict[str, Any], str]:
    rr = summarize_rr(trace_dir)
    log = summarize_log(log_path)

    print(f"Trace directory: {trace_dir}")
    print(f"RR files/events: {rr['files']}/{rr['events']}")
    print_counter("HTTP statuses", rr["http_statuses"])
    print_counter("MAPI response codes", rr["response_codes"])
    print_counter("Non-zero MAPI response codes", rr["nonzero_response_codes"])
    print_counter("ROP parse errors", rr["parse_errors"])
    print_counter("Inbound Execute ROP sequences", rr["request_sequences"], limit=20)

    sessions = rr["sessions"]
    if sessions:
        longest_session = max(sessions, key=lambda key: len(sessions[key]))
        print(f"Last ROPs in busiest session ({longest_session}):")
        for names in sessions[longest_session]:
            print(f"  {names}")

    if log_path:
        print(f"Journal log: {log_path}")
        if log["build"]:
            print(
                "Build: "
                f"version={log['build'].get('package_version', '')};"
                f"commit={log['build'].get('git_commit', '')};"
                f"time={log['build'].get('git_commit_time', '')};"
                f"dirty={format_build_dirty(log['build'].get('git_dirty'))}"
            )
        print(f"Journal lines/execute events: {log['lines']}/{log['execute_events']}")
        print_counter("Startup first missing gates", log["startup_missing_gates"])
        print_counter("Execute stall names", log["stall_warnings"])
        print_counter("Journal ROP sequence signatures", log["sequence_counts"], limit=20)
        print_counter("Unknown GetProps tags", log["unknown_getprops_tags"], limit=20)
        print_counter("Zero-default tags", log["zero_default_tags"], limit=20)
        print_counter("Stale default-view owner states", log["stale_default_view_states"])
        print(f"Visible Inbox release-before-QueryRows events: {log['visible_release_without_query_rows']}")
        print(f"Raw UMOLK <xml/> placeholder hits: {log['raw_umolk_placeholder']}")
        if log["last_execute"]:
            print("Last Execute:")
            for key, value in log["last_execute"].items():
                print(f"  {key}: {value}")

    verdict = verdict_for_summary(rr, log, log_path)
    print(f"Verdict: {verdict}")
    return rr, log, verdict


def verdict_for_summary(
    rr: dict[str, Any], log: dict[str, Any], log_path: Path | None
) -> str:
    if rr["nonzero_response_codes"] or rr["parse_errors"]:
        return "RR trace shows protocol/parse errors before client stoppage."
    if log_path and (
        log["visible_release_without_query_rows"]
        or log["raw_umolk_placeholder"]
        or log["stale_default_view_states"]
    ):
        return "transport is clean; journal diagnostics contain actionable MAPI/view issues."
    if log_path and log["stall_warnings"]:
        return "transport is clean; startup stall diagnostics identify a server-side MAPI bootstrap stop."
    if log_path and log["startup_missing_gates"]:
        return "transport is clean; startup gate diagnostics identify the next missing Outlook bootstrap step."
    return "transport is clean; if Outlook still crashes, ETL/client crash data may be useful."


def print_batch_summary(
    trace_root: Path, logs_root: Path, current_build: str | None
) -> int:
    runs = [path for path in sorted(trace_root.iterdir()) if path.is_dir()]
    logs_by_stamp = indexed_log_files(logs_root)
    aggregate_missing_gates: Counter[str] = Counter()
    aggregate_stalls: Counter[str] = Counter()
    aggregate_sequences: Counter[str] = Counter()
    aggregate_unknown_tags: Counter[str] = Counter()
    aggregate_nonzero_response_codes: Counter[str] = Counter()
    build_issue_counts: Counter[tuple[str, str]] = Counter()
    actionable_runs = 0

    print(
        "run,matched_log,build_commit,build_dirty,build_scope,rr_events,nonzero_mapi,parse_errors,missing_gate,"
        "visible_release_before_query,raw_umolk,stale_default_view,verdict"
    )
    for trace_dir in runs:
        log_path = matching_log_for_run(trace_dir.name, logs_by_stamp)
        rr = summarize_rr(trace_dir)
        log = summarize_log(log_path)
        verdict = verdict_for_summary(rr, log, log_path)
        if "ETL/client crash data" not in verdict:
            actionable_runs += 1
        aggregate_missing_gates.update(log["startup_missing_gates"])
        aggregate_stalls.update(log["stall_warnings"])
        aggregate_sequences.update(log["sequence_counts"])
        aggregate_unknown_tags.update(log["unknown_getprops_tags"])
        aggregate_nonzero_response_codes.update(rr["nonzero_response_codes"])
        build_commit = str(log["build"].get("git_commit", "unknown"))
        build_dirty = format_build_dirty(log["build"].get("git_dirty"))
        for issue in issue_buckets(rr, log, log_path):
            build_issue_counts[(f"{build_commit}/{build_dirty}", issue)] += 1
        missing_gate = (
            log["startup_missing_gates"].most_common(1)[0][0]
            if log["startup_missing_gates"]
            else "none"
        )
        build_scope = build_scope_for(
            log["build"].get("git_commit"),
            log["build"].get("git_dirty"),
            current_build,
        )
        print(
            ",".join(
                [
                    trace_dir.name,
                    log_path.name if log_path else "",
                    str(log["build"].get("git_commit", "")),
                    build_dirty,
                    build_scope,
                    str(rr["events"]),
                    str(sum(rr["nonzero_response_codes"].values())),
                    str(sum(rr["parse_errors"].values())),
                    missing_gate,
                    str(log["visible_release_without_query_rows"]),
                    str(log["raw_umolk_placeholder"]),
                    str(sum(log["stale_default_view_states"].values())),
                    verdict,
                ]
            )
        )

    print()
    print(f"Runs scanned: {len(runs)}")
    print(f"Runs with actionable diagnostics: {actionable_runs}")
    print_counter("Aggregate non-zero MAPI response codes", aggregate_nonzero_response_codes)
    print_counter("Aggregate startup first missing gates", aggregate_missing_gates)
    print_counter("Aggregate execute stall names", aggregate_stalls)
    print_counter("Aggregate journal ROP sequence signatures", aggregate_sequences, limit=20)
    print_counter("Aggregate unknown GetProps tags", aggregate_unknown_tags, limit=20)
    print_build_issue_counts(build_issue_counts)
    return 0


def format_build_dirty(value: object) -> str:
    if value in (None, ""):
        return "clean"
    return str(value)


def build_scope_for(
    build_commit: object, git_dirty: object, current_build: str | None
) -> str:
    if not build_commit:
        return "unknown-build"
    if current_build:
        if str(build_commit).startswith(current_build):
            return "current-dirty-build" if format_build_dirty(git_dirty) != "clean" else "current-clean-build"
        return "old-build"
    return ""


def issue_buckets(
    rr: dict[str, Any], log: dict[str, Any], log_path: Path | None
) -> list[str]:
    issues: list[str] = []
    if rr["nonzero_response_codes"]:
        issues.append("nonzero_mapi_response")
    if rr["parse_errors"]:
        issues.append("rop_parse_error")
    if log["visible_release_without_query_rows"]:
        issues.append("visible_inbox_release_before_query_rows")
    if log["raw_umolk_placeholder"]:
        issues.append("raw_umolk_placeholder")
    if log["stale_default_view_states"]:
        issues.append("stale_default_view_state")
    if log["stall_warnings"]:
        for name, _count in log["stall_warnings"].most_common(2):
            issues.append(f"stall:{name}")
    if log_path and log["startup_missing_gates"]:
        gate = log["startup_missing_gates"].most_common(1)[0][0]
        issues.append(f"missing_gate:{gate}")
    if not issues:
        issues.append("no_server_issue_detected")
    return issues


def print_build_issue_counts(counter: Counter[tuple[str, str]]) -> None:
    print("Issue buckets by build")
    if not counter:
        print("  none")
        return
    for (build, issue), count in sorted(
        counter.items(), key=lambda item: (-item[1], item[0][0], item[0][1])
    )[:40]:
        print(f"  {build},{issue}: {count}")


def indexed_log_files(logs_root: Path) -> dict[str, Path]:
    logs: dict[str, Path] = {}
    for path in logs_root.glob("LPE_last_*.log"):
        match = RUN_STAMP_RE.search(path.stem)
        if match:
            logs[match.group(1)] = path
    return logs


def matching_log_for_run(run_name: str, logs_by_stamp: dict[str, Path]) -> Path | None:
    if run_name in logs_by_stamp:
        return logs_by_stamp[run_name]
    run_stamp = parse_stamp(run_name)
    if run_stamp is None:
        return None
    best: tuple[float, Path] | None = None
    for stamp, path in logs_by_stamp.items():
        log_stamp = parse_stamp(stamp)
        if log_stamp is None or log_stamp.date() != run_stamp.date():
            continue
        delta_seconds = abs((log_stamp - run_stamp).total_seconds())
        if delta_seconds <= 180 and (best is None or delta_seconds < best[0]):
            best = (delta_seconds, path)
    return best[1] if best else None


def parse_stamp(value: str) -> datetime | None:
    match = RUN_STAMP_RE.search(value)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M")
    except ValueError:
        return None


def main() -> int:
    args = parse_args()
    if args.all:
        return print_batch_summary(args.trace_dir, args.logs_root, args.current_build)
    print_single_summary(args.trace_dir, args.log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
