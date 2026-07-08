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
FRAME_PREVIEW_RE = re.compile(r"preview=([0-9a-fA-F]+)")
UMOLK_USER_OPTIONS_CLASS = "IPM.Configuration.UMOLK.UserOptions"
UMOLK_MANDATORY_DICTIONARY_TAGS = {
    "0x7c060003": "PidTagRoamingDatatypes",
    "0x7c070102": "PidTagRoamingDictionary",
}
KNOWN_STATIC_GETPROPS_TAGS = {
    # Microsoft-defined message/store properties that older LPE builds logged as
    # unknown before the debug-name table caught up with Outlook traces.
    "0x004b001f",  # PidTagOriginalMessageClass
    "0x0040001f",  # PidTagReceivedByName
    "0x00410102",  # PidTagSentRepresentingEntryId
    "0x0042001f",  # PidTagSentRepresentingName
    "0x00430102",  # PidTagReceivedRepresentingEntryId
    "0x0044001f",  # PidTagReceivedRepresentingName
    "0x003b0102",  # PidTagSentRepresentingSearchKey
    "0x003f0102",  # PidTagReceivedByEntryId
    "0x00510102",  # PidTagReceivedBySearchKey
    "0x00520102",  # PidTagReceivedRepresentingSearchKey
    "0x0064001f",  # PidTagSentRepresentingAddressType
    "0x0065001f",  # PidTagSentRepresentingEmailAddress
    "0x0070001f",  # PidTagConversationTopic
    "0x00710102",  # PidTagConversationIndex
    "0x0075001f",  # PidTagReceivedByAddressType
    "0x0076001f",  # PidTagReceivedByEmailAddress
    "0x0077001f",  # PidTagReceivedRepresentingAddressType
    "0x0078001f",  # PidTagReceivedRepresentingEmailAddress
    "0x0c190102",  # PidTagSenderEntryId
    "0x0c1a001f",  # PidTagSenderName
    "0x0c1d0102",  # PidTagSenderSearchKey
    "0x0c1e001f",  # PidTagSenderAddressType
    "0x0c1f001f",  # PidTagSenderEmailAddress
    "0x0002000b",  # PidTagAlternateRecipientAllowed
    "0x0005000b",  # PidTagAutoForwarded
    "0x000f0040",  # PidTagDeferredDeliveryTime
    "0x00150040",  # PidTagExpiryTime
    "0x0023000b",  # PidTagOriginatorDeliveryReportRequested
    "0x00250102",  # PidTagParentKey
    "0x0029000b",  # PidTagReadReceiptRequested
    "0x002b000b",  # PidTagRecipientReassignmentProhibited
    "0x00300040",  # PidTagReplyTime
    "0x00310102",  # PidTagReportTag
    "0x00320040",  # PidTagReportTime
    "0x004c0102",  # PidTagOriginalAuthorEntryId
    "0x004d001f",  # PidTagOriginalAuthorName
    "0x004e0040",  # PidTagOriginalSubmitTime
    "0x0049001f",  # PidTagOriginalSubject
    "0x005a001f",  # PidTagOriginalSenderName
    "0x004f0102",  # PidTagReplyRecipientEntries
    "0x0050001f",  # PidTagReplyRecipientNames
    "0x0063000b",  # PidTagResponseRequested
    "0x00600040",  # PidTagStartDate
    "0x00610040",  # PidTagEndDate
    "0x00620003",  # PidTagOwnerAppointmentId
    "0x0072001f",  # PidTagOriginalDisplayBcc
    "0x0073001f",  # PidTagOriginalDisplayCc
    "0x0074001f",  # PidTagOriginalDisplayTo
    "0x007d001f",  # PidTagTransportMessageHeaders
    "0x0080001f",  # PidTagReportDisposition
    "0x0e01000b",  # PidTagDeleteAfterSubmit
    "0x0e02001f",  # PidTagDisplayBcc
    "0x0e060040",  # PidTagMessageDeliveryTime
    "0x0e03001f",  # PidTagDisplayCc
    "0x0e04001f",  # PidTagDisplayTo
    "0x0e070003",  # PidTagMessageFlags
    "0x0e080003",  # PidTagMessageSize
    "0x0e170003",  # PidTagMessageStatus
    "0x0e1b000b",  # PidTagHasAttachments
    "0x0e1f000b",  # PidTagRtfInSync
    "0x0c17000b",  # PidTagReplyRequested
    "0x10800003",  # PidTagIconIndex
    "0x10810003",  # PidTagLastVerbExecuted
    "0x10820040",  # PidTagLastVerbExecutionTime
    "0x1035001f",  # PidTagInternetMessageId
    "0x1039001f",  # PidTagInternetReferences
    "0x1042001f",  # PidTagInReplyToId
    "0x0e28001f",  # PidTagPrimarySendAccount
    "0x0e29001f",  # PidTagNextSendAcct
    "0x10900003",  # PidTagFlagStatus
    "0x10910040",  # PidTagFlagCompleteTime
    "0x0e2b0003",  # PidTagTodoItemFlags
    "0x0e2c0102",  # PidTagSwappedToDoStore
    "0x0e2d0102",  # PidTagSwappedToDoData
    "0x0f030102",  # Outlook messages view binary descriptor blob
    "0x10950003",  # PidTagFollowupIcon
    "0x10960003",  # PidTagBlockStatus
    "0x0ff70003",  # PidTagAccessLevel
    "0x3fef0040",  # PidTagDeferredSendTime
    "0x3fde0003",  # PidTagInternetCodepage
    "0x3ff10003",  # PidTagMessageLocaleId
    "0x3711001f",  # PidTagAttachContentBase
    "0x59020003",  # PidTagInternetMailOverrideFormat
    "0x59090003",  # PidTagMessageEditorFormat
    "0x3ffa001f",  # PidTagLastModifierName
    "0x7d01000b",  # PidTagProcessed
    "0x00170003",  # PidTagImportance
    "0x00260003",  # PidTagPriority
    "0x002e0003",  # PidTagOriginalSensitivity
    "0x00360003",  # PidTagSensitivity
    "0x00390040",  # PidTagClientSubmitTime
    "0x30130102",  # PidTagConversationId
    "0x3016000b",  # PidTagConversationIndexTracking
    "0x300b0102",  # PidTagSearchKey
    "0x30180102",  # PidTagArchiveTag
    "0x30190102",  # PidTagPolicyTag
    "0x301a0003",  # PidTagRetentionPeriod
    "0x301b0102",  # PidTagStartDateEtc
    "0x301c0040",  # PidTagRetentionDate
    "0x301d0003",  # PidTagRetentionFlags
    "0x301e0003",  # PidTagArchivePeriod
    "0x301f0040",  # PidTagArchiveDate
    "0x30070040",  # PidTagCreationTime
    "0x0e210003",  # PidTagAttachNumber
    "0x674000fb",  # PidTagSentMailSvrEID
    "0x10090102",  # PidTagRtfCompressed
    "0x5d01001f",  # PidTagSenderSmtpAddress
    "0x5d02001f",  # PidTagSentRepresentingSmtpAddress
    "0x836b001f",  # PidNameContentType
}
KNOWN_BACKED_DESCRIPTOR_TAGS = {
    # Columns already backed by the current Outlook view/table projections.
    # Keep raw descriptor-gap counters visible, but do not turn old-build gaps
    # for these columns into a current actionable issue bucket.
    "0x001a001f",  # PidTagMessageClass
    "0x00170003",  # PidTagImportance
    "0x0037001f",  # PidTagSubject
    "0x00390040",  # PidTagClientSubmitTime
    "0x0042001f",  # PidTagSentRepresentingName
    "0x00710102",  # PidTagConversationIndex
    "0x0e060040",  # PidTagMessageDeliveryTime
    "0x0e070003",  # PidTagMessageFlags
    "0x0e080003",  # PidTagMessageSize
    "0x0e170003",  # PidTagMessageStatus
    "0x0e1b000b",  # PidTagHasAttachments
    "0x0e1d001f",  # PidTagNormalizedSubject
    "0x0e69000b",  # PidTagRead
    "0x0fff0102",  # PidTagEntryId
    "0x0ff60102",  # PidTagInstanceKey
    "0x12130003",  # Outlook compact-view auxiliary flags
    "0x0f030102",  # Outlook messages view binary descriptor blob
    "0x30080040",  # PidTagLastModificationTime
    "0x67480014",  # PidTagFolderId
    "0x674a0014",  # PidTagMid
    "0x674d0014",  # PidTagInstID
    "0x674e0003",  # PidTagInstanceNum
    "0x67a40014",  # PidTagCn
    "0x65e00102",  # Outlook associated config binary stream
    "0x65e10102",  # Outlook associated config binary stream
    "0x65e20102",  # Outlook associated config binary stream
    "0x65e30102",  # Outlook associated config binary stream
    "0x685d0003",  # OutlookConfigurationStamp
    "0x7c060003",  # PidTagRoamingDatatypes
    "0x82050003",  # PidLidBusyStatus
    "0x8208001f",  # PidLidLocation
    "0x85100003",  # PidLidSideEffects
    "0x8514000b",  # Outlook common flag used in default views
    "0x85160040",  # PidLidCommonStart
    "0x85170040",  # PidLidCommonEnd
    "0x85780003",  # Outlook calendar auxiliary status
    "0x85ef000b",  # PidLidOutlookCommon85EF
}
ACTIONABLE_ZERO_DEFAULT_TAGS = {}


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
        "session_event_tails": defaultdict(lambda: deque(maxlen=12)),
        "emsmdb_session_event_tails": defaultdict(lambda: deque(maxlen=12)),
        "parse_errors": Counter(),
        "request_sequences": Counter(),
        "setcolumns_release_response_frames": Counter(),
        "setcolumns_release_response_previews": Counter(),
        "setcolumns_release_response_handle_classifications": Counter(),
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
                summary["session_event_tails"][session_id].append(
                    rr_event_tail_summary(event, metadata)
                )
                if event.get("endpoint") == "emsmdb":
                    summary["emsmdb_session_event_tails"][session_id].append(
                        rr_event_tail_summary(event, metadata)
                    )
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
                if event.get("direction") == "outbound" and event.get("phase") == "Execute":
                    names = str(metadata.get("request_rop_names") or "")
                    if names == "SetColumns,Release,Release,Release":
                        frames = str(metadata.get("response_rop_frames") or "missing")
                        preview = str(metadata.get("response_rop_buffer_preview") or "missing")
                        summary["setcolumns_release_response_frames"][frames] += 1
                        summary["setcolumns_release_response_previews"][preview] += 1
                        summary["setcolumns_release_response_handle_classifications"][
                            classify_rr_setcolumns_release_response(metadata)
                        ] += 1
    return summary


def classify_rr_setcolumns_release_response(metadata: dict[str, Any]) -> str:
    response_handles = parse_rr_response_handle_table(metadata)
    if not response_handles:
        return "response_handle_table_unknown"
    if response_handles[0].lower() in {"0x00000000", "0xffffffff"}:
        return "released_slot_invalidated_in_response_handle_table"
    request_handles = parse_handle_table_summary(str(metadata.get("request_handle_table") or ""))
    if request_handles and response_handles[0].lower() == request_handles[0].lower():
        return "released_slot_reused_in_response_handle_table"
    return "released_slot_non_request_handle_in_response_handle_table"


def parse_rr_response_handle_table(metadata: dict[str, Any]) -> list[str]:
    try:
        handle_bytes = int(str(metadata.get("response_handle_table_bytes") or "0"))
    except ValueError:
        return []
    preview = str(metadata.get("response_rop_buffer_preview") or "")
    if handle_bytes <= 0 or len(preview) < handle_bytes * 2:
        return []
    handle_hex = preview[-handle_bytes * 2 :]
    handles = []
    for offset in range(0, len(handle_hex), 8):
        chunk = handle_hex[offset : offset + 8]
        if len(chunk) != 8:
            return []
        value = int.from_bytes(bytes.fromhex(chunk), "little")
        handles.append(f"0x{value:08x}")
    return handles


def rr_event_tail_summary(event: dict[str, Any], metadata: dict[str, Any]) -> str:
    parts = [
        f"{event.get('direction', '')}:{event.get('endpoint', '')}:{event.get('phase', '')}",
    ]
    request_id = metadata.get("mapi_request_id")
    if request_id:
        parts.append(f"request={request_id}")
    names = metadata.get("request_rop_names")
    if names:
        parts.append(f"rops={names}")
    status = event.get("response_status") or event.get("status")
    if status not in (None, ""):
        parts.append(f"http={status}")
    response_code = metadata.get("mapi_response_code")
    if response_code not in (None, ""):
        parts.append(f"mapi={response_code}")
    payload_bytes = (
        event.get("request_body_bytes")
        or event.get("response_body_bytes")
        or event.get("raw_payload_bytes")
        or event.get("payload_bytes")
    )
    if payload_bytes not in (None, ""):
        parts.append(f"bytes={payload_bytes}")
    return ";".join(str(part) for part in parts if part)


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
        "query_rows_response_frames": Counter(),
        "visible_release_without_query_rows": 0,
        "visible_inbox_query_rows": Counter(),
        "visible_inbox_query_rows_contexts": Counter(),
        "raw_visible_release_marker_lines": 0,
        "raw_umolk_placeholder": 0,
        "stale_default_view_states": Counter(),
        "visible_release_contexts": set(),
        "visible_release_classifications": Counter(),
        "visible_release_request_shapes": Counter(),
        "visible_release_setcolumns_shapes": Counter(),
        "visible_release_pre_release_states": Counter(),
        "visible_release_handle_slots": Counter(),
        "setcolumns_release_response_frames": Counter(),
        "setcolumns_release_response_handle_tables": Counter(),
        "setcolumns_release_response_handle_classifications": Counter(),
        "visible_release_descriptor_windows": Counter(),
        "visible_release_descriptor_contract_issues": Counter(),
        "common_view_descriptor_getprops": Counter(),
        "common_view_descriptor_getprops_issues": Counter(),
        "common_view_descriptor_getprops_contexts": set(),
        "post_visible_release_followups": Counter(),
        "post_visible_release_terminal_events": Counter(),
        "post_visible_release_terminal_tail": deque(maxlen=12),
        "post_visible_release_terminal_contexts": set(),
        "post_visible_release_hierarchy_query_position_max": 0,
        "umolk_dictionary_shapes": Counter(),
        "umolk_dictionary_issues": Counter(),
        "default_view_folder_open_without_rows": Counter(),
        "default_view_query_position_without_rows": Counter(),
        "default_view_query_position_without_rows_contexts": set(),
        "default_view_id_owners": defaultdict(set),
        "default_view_id_collision_contexts": set(),
        "default_view_id_collisions": Counter(),
        "calendar_zero_duration_timed_query_position_rows": Counter(),
        "post_calendar_query_position_named_property_probes": Counter(),
        "descriptor_gap_windows": Counter(),
        "default_view_descriptor_identity_columns": Counter(),
        "folder_local_default_view_visibility": Counter(),
        "folder_local_default_view_visibility_contexts": Counter(),
        "stale_default_view_contexts": set(),
        "unknown_getprops_tags": Counter(),
        "unknown_getprops_contexts": Counter(),
        "unknown_defaulted_getprops_tags": Counter(),
        "unknown_defaulted_getprops_contexts": Counter(),
        "problem_getprops_tags": Counter(),
        "problem_getprops_contexts": Counter(),
        "umolk_problem_getprops_tags": Counter(),
        "umolk_problem_getprops_contexts": Counter(),
        "umolk_getprops_request_ids": set(),
        "associated_config_optional_defaulted_getprops_tags": Counter(),
        "associated_config_optional_defaulted_getprops_contexts": Counter(),
        "resolved_named_getprops_tags": set(),
        "zero_default_tags": Counter(),
        "hierarchy_query_windows": Counter(),
        "hierarchy_query_samples": deque(maxlen=8),
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
            record_default_view_folder_open_without_rows(summary, fields)
            record_query_position_wire_fields(summary, fields)
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
                record_query_rows_response_frames(summary, fields, str(signature or "unknown"))
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
                record_setcolumns_release_response(summary, fields)
                record_post_visible_release_followup(summary, fields)
                record_umolk_dictionary_shapes(
                    summary,
                    str(fields.get("last_outlook_umolk_getprops_materialization_context") or ""),
                )
            elif message == "rca debug mapi outlook surface getprops contract":
                contract = str(fields.get("getprops_contract") or "")
                inspect_contract(summary, contract, fields)
                record_common_view_descriptor_getprops(
                    summary,
                    str(fields.get("common_view_descriptor_getprops_contract") or ""),
                    fields,
                )
            elif message == "rca debug mapi get properties specific":
                record_common_view_descriptor_getprops(
                    summary,
                    str(fields.get("common_view_descriptor_getprops_contract") or ""),
                    fields,
                )
            elif message == "rca debug mapi get properties named property context":
                record_resolved_named_property_context(summary, fields)
            elif message == "rca debug outlook view descriptor getprops":
                record_common_view_descriptor_getprops(summary, "", fields)
            elif message == "rca debug mapi setcolumns release response framing":
                record_setcolumns_release_response(summary, fields)
            elif message == "rca debug mapi visible inbox query rows tracked":
                record_visible_inbox_query_rows(summary, fields)
            elif message == "rca debug mapi post calendar query position named property probe":
                record_post_calendar_query_position_named_property_probe(summary, fields)
            elif message == "rca debug mapi umolk getprops materialization":
                request_id = fields.get("mapi_request_id")
                if request_id:
                    summary["umolk_getprops_request_ids"].add(str(request_id))
                record_umolk_dictionary_shapes(
                    summary,
                    str(fields.get("umolk_getprops_materialization_context") or ""),
                )
            release_context = fields.get("release_without_query_rows_context")
            if isinstance(release_context, str) and release_context:
                record_visible_release_context(summary, release_context)
            if message == "rca debug outlook hierarchy table query rows response":
                record_hierarchy_query_window(summary, fields)
            for key in (
                "view_handoff_table_contract",
                "folder_local_default_view_visibility",
                "descriptor_behavior",
                "descriptor_query_window",
            ):
                value = fields.get(key)
                if key == "folder_local_default_view_visibility" and isinstance(value, str):
                    record_folder_local_default_view_visibility(summary, fields)
                if isinstance(value, str) and "selected_missing_descriptor_columns=" in value:
                    record_descriptor_gap(summary, value, fields)
    summary["visible_release_without_query_rows"] = len(summary["visible_release_contexts"])
    if summary["visible_release_without_query_rows"] == 0:
        summary["visible_release_without_query_rows"] = summary[
            "raw_visible_release_marker_lines"
        ]
    for tag in summary["resolved_named_getprops_tags"]:
        summary["unknown_getprops_tags"].pop(tag, None)
    summary["unknown_getprops_contexts"] = Counter(
        {
            context: count
            for context, count in summary["unknown_getprops_contexts"].items()
            if context.split(";", 1)[0] not in summary["resolved_named_getprops_tags"]
        }
    )
    summary["unknown_defaulted_getprops_contexts"] = Counter(
        {
            context: count
            for context, count in summary["unknown_defaulted_getprops_contexts"].items()
            if context.split(";", 1)[0] not in summary["resolved_named_getprops_tags"]
        }
    )
    return summary


def record_query_rows_response_frames(
    summary: dict[str, Any], fields: dict[str, Any], signature: str
) -> None:
    frames = str(fields.get("response_rop_frames") or "")
    if not frames:
        return
    request_id = str(fields.get("mapi_request_id") or "unknown")
    handle_table = str(
        fields.get("request_handle_table")
        or fields.get("input_handle_table_summary")
        or "unknown"
    )
    buffer_text_hint = query_rows_preview_text_hint(
        str(fields.get("response_rop_buffer_preview") or "")
    )
    for frame in frames.split("|"):
        if not frame.startswith("0x15@"):
            continue
        match = FRAME_PREVIEW_RE.search(frame)
        preview = match.group(1) if match else None
        if not preview or len(preview) < 18:
            continue
        try:
            origin = int(preview[12:14], 16)
            row_count = int.from_bytes(bytes.fromhex(preview[14:18]), "little")
        except ValueError:
            continue
        preview_text = query_rows_preview_text_hint(preview)
        if preview_text == "none":
            preview_text = buffer_text_hint
        key = (
            f"signature={signature};handles={handle_table};"
            f"rows={row_count};origin=0x{origin:02x};text={preview_text}"
        )
        summary["query_rows_response_frames"][key] += 1
        if row_count:
            summary["query_rows_response_frames"][
                f"nonempty;request={request_id};{key}"
            ] += 1


def query_rows_preview_text_hint(preview: str) -> str:
    try:
        data = bytes.fromhex(preview)
    except ValueError:
        return "none"
    text = data.decode("utf-16-le", errors="ignore")
    runs: list[str] = []
    current: list[str] = []
    for char in text:
        if 32 <= ord(char) < 127:
            current.append(char)
            continue
        if len(current) >= 4:
            runs.append("".join(current))
        current = []
    if len(current) >= 4:
        runs.append("".join(current))
    if not runs:
        return "none"
    return ",".join(runs)[:120]


def record_visible_inbox_query_rows(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    context = str(fields.get("query_rows_context") or "")
    request_id = str(fields.get("mapi_request_id") or "unknown")
    if not context:
        return
    returned = field_in_semicolon_text(context, "returned") or "unknown"
    position = field_in_semicolon_text(context, "position") or "unknown"
    requested = field_in_semicolon_text(context, "requested_row_count") or "unknown"
    key = f"returned={returned};position={position};requested={requested}"
    summary["visible_inbox_query_rows"][key] += 1
    summary["visible_inbox_query_rows_contexts"][f"request={request_id};{context}"] += 1


def record_folder_local_default_view_visibility(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    context = str(fields.get("folder_local_default_view_visibility") or "")
    if not context:
        return
    role = field_in_semicolon_text(context, "role") or "unknown"
    name = field_in_semicolon_text(context, "name") or "unknown"
    present = field_in_semicolon_text(context, "present") or "unknown"
    key = f"role={role};name={name};present={present}"
    summary["folder_local_default_view_visibility"][key] += 1
    if present == "false":
        summary["folder_local_default_view_visibility_contexts"][context] += 1


def record_umolk_dictionary_shapes(summary: dict[str, Any], text: str) -> None:
    shape = first_field(text, "dictionary_shape")
    if shape:
        summary["umolk_dictionary_shapes"][shape] += 1
    record_umolk_dictionary_contract(summary, text)


def record_umolk_dictionary_contract(summary: dict[str, Any], text: str) -> None:
    if not text:
        return
    requested = {tag.lower() for tag in tags_after(text, "tags=")}
    if not requested:
        return
    returned = {tag.lower() for tag in tags_after(text, "values=")}
    problem = {tag.lower() for tag in problem_tags_after(text, "first_problem_tags=")}
    missing = []
    for tag, name in UMOLK_MANDATORY_DICTIONARY_TAGS.items():
        if tag not in requested:
            continue
        if tag not in returned or tag in problem:
            missing.append(f"{tag}:{name}")
    if not missing:
        return
    request_id = first_field(text, "request_id") or "unknown"
    config_id = first_field(text, "config") or "unknown"
    config_class = first_field(text, "class") or "unknown"
    summary["umolk_dictionary_issues"][
        f"missing={','.join(missing)};request={request_id};"
        f"config={config_id};class={config_class}"
    ] += 1


def record_common_view_descriptor_getprops(
    summary: dict[str, Any], contract: str, fields: dict[str, Any]
) -> None:
    if not contract and not field_text(fields, "requested_view_descriptor_contract"):
        return

    found = first_field(contract, "found")
    view_name = first_field(contract, "view_name") or field_text(fields, "view_name")
    folder_id = first_field(contract, "folder_id") or field_text(fields, "folder_id")
    view_id = first_field(contract, "view_id") or field_text(fields, "view_message_id")
    requested_tags = first_field(contract, "requested_descriptor_tags") or field_text(
        fields, "requested_property_tags"
    )
    requested_contract = field_text(fields, "requested_view_descriptor_contract")
    response_values = first_field(contract, "response_values") or field_text(
        fields, "requested_view_descriptor_response_values"
    )

    key = (
        f"found={found or 'unknown'};view={view_name or 'unknown'};"
        f"folder={folder_id or 'unknown'};view_id={view_id or 'unknown'};"
        f"requested={requested_tags or requested_contract or 'unknown'}"
    )
    if key in summary["common_view_descriptor_getprops_contexts"]:
        return
    summary["common_view_descriptor_getprops_contexts"].add(key)
    summary["common_view_descriptor_getprops"][key] += 1

    if found == "false":
        summary["common_view_descriptor_getprops_issues"][
            f"missing_view_message;{key}"
        ] += 1
        return

    missing_response_parts: list[str] = []
    if requested_contract:
        for name in ("version", "name", "binary", "strings"):
            if first_field(requested_contract, name) == "true" and (
                not response_values or name not in response_values.lower()
            ):
                missing_response_parts.append(name)
    if missing_response_parts:
        summary["common_view_descriptor_getprops_issues"][
            f"missing_response_values={','.join(missing_response_parts)};{key}"
        ] += 1

    descriptor_contract_issues = inbox_compact_descriptor_getprops_contract_issues(
        contract, fields, view_name, folder_id
    )
    if descriptor_contract_issues:
        summary["common_view_descriptor_getprops_issues"][
            f"descriptor_contract={','.join(descriptor_contract_issues)};{key}"
        ] += 1


def inbox_compact_descriptor_getprops_contract_issues(
    contract: str,
    fields: dict[str, Any],
    view_name: str | None,
    folder_id: str | None,
) -> list[str]:
    if (view_name or "").lower() != "compact":
        return []
    if (folder_id or "").lower() != "0x0000000000050001":
        return []

    issues = []
    column_count = first_field(contract, "descriptor_column_count") or field_text(
        fields, "descriptor_column_count"
    )
    if column_count and column_count != "11":
        issues.append(f"columns={column_count}")
    terminators = first_field(contract, "descriptor_strings_terminators") or field_text(
        fields, "descriptor_strings_terminators"
    )
    if terminators and terminators != "11":
        issues.append(f"string_terminators={terminators}")
    starts = first_field(contract, "descriptor_strings_starts_with_terminator") or field_text(
        fields, "descriptor_strings_starts_with_terminator"
    )
    if starts and starts.lower() != "true":
        issues.append(f"strings_start={starts}")
    ends = first_field(contract, "descriptor_strings_ends_with_terminator") or field_text(
        fields, "descriptor_strings_ends_with_terminator"
    )
    if ends and ends.lower() != "true":
        issues.append(f"strings_end={ends}")
    trailing_nul = first_field(contract, "descriptor_strings_trailing_nul") or field_text(
        fields, "descriptor_strings_trailing_nul"
    )
    if trailing_nul and trailing_nul.lower() != "false":
        issues.append(f"strings_trailing_nul={trailing_nul}")
    return issues


def record_post_visible_release_followup(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    hierarchy_count = int_field(
        fields, "post_visible_release_hierarchy_query_position_count"
    )
    if hierarchy_count > summary["post_visible_release_hierarchy_query_position_max"]:
        summary["post_visible_release_hierarchy_query_position_max"] = hierarchy_count
    if hierarchy_count > 0:
        summary["post_visible_release_followups"][
            "hierarchy_query_position_after_visible_release"
        ] += 1

    create_save_count = int_field(
        fields, "post_visible_inbox_release_create_save_batch_count"
    )
    if create_save_count > 0:
        summary["post_visible_release_followups"][
            "create_save_batch_after_visible_release"
        ] += 1

    if is_truthy(fields.get("default_view_normal_query_rows_observed")) and not is_truthy(
        fields.get("normal_inbox_contents_query_rows_observed")
    ):
        summary["post_visible_release_followups"][
            "default_view_rows_elsewhere_without_inbox_rows"
        ] += 1

    umolk_probe_count = int_field(fields, "outlook_umolk_named_property_probe_count")
    umolk_not_found_count = int_field(fields, "outlook_umolk_getprops_not_found_count")
    if umolk_probe_count > 0 and umolk_not_found_count == 0:
        summary["post_visible_release_followups"][
            "umolk_materialized_before_stop"
        ] += 1


def int_field(fields: dict[str, Any], key: str) -> int:
    value = fields.get(key)
    if isinstance(value, bool):
        return int(value)
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def int_text_field(text: str, key: str) -> int:
    try:
        return int(first_field(text, key) or "0")
    except ValueError:
        return 0


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


def record_hierarchy_query_window(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    row_summary = str(fields.get("hierarchy_wire_row_summary") or "")
    first_row = first_hierarchy_row(row_summary)
    role = field_text(fields, "folder_role")
    queried_position = field_text(fields, "queried_position")
    row_count = field_text(fields, "response_row_count")
    origin = field_text(fields, "response_origin_name") or field_text(
        fields, "response_origin"
    )
    key = (
        f"role={role};queried={queried_position};rows={row_count};"
        f"origin={origin};first={first_row}"
    )
    summary["hierarchy_query_windows"][key] += 1
    if row_summary and len(summary["hierarchy_query_samples"]) < 8:
        summary["hierarchy_query_samples"].append(key)


def field_text(fields: dict[str, Any], key: str) -> str:
    value = fields.get(key)
    if value is None:
        return ""
    return str(value)


def first_hierarchy_row(row_summary: str) -> str:
    if not row_summary or "index=0;" not in row_summary:
        return "none"
    first = row_summary.split("index=0;", 1)[1].split("|", 1)[0]
    parts: list[str] = []
    for key in ("id", "class", "name"):
        value = first_field(first, key)
        if value:
            parts.append(f"{key}={value}")
    return "/".join(parts) if parts else "decoded"


def inspect_view_trace(summary: dict[str, Any], trace_events: str) -> None:
    if not trace_events:
        return
    after_visible_release = False
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
        if segment.startswith("visible_inbox_release_without_query_rows:"):
            after_visible_release = True
            record_visible_release_context(summary, segment)
        if (
            after_visible_release
            or "after_visible_inbox_release_without_query_rows=true" in segment
        ):
            record_post_visible_release_terminal_event(summary, segment)
        record_default_view_query_position_without_rows(summary, segment)
        record_default_view_id_collision(summary, segment)
        record_default_view_descriptor_identity_columns(summary, segment)
        record_descriptor_gap(summary, segment)
        inspect_contract(summary, segment)


def record_visible_release_context(summary: dict[str, Any], text: str) -> None:
    if text.startswith("visible_inbox_release_without_query_rows:"):
        segment = text
    else:
        segment = f"visible_inbox_release_without_query_rows:{text}"
    if segment in summary["visible_release_contexts"]:
        return
    summary["visible_release_contexts"].add(segment)
    record_visible_release_classification(summary, segment)
    record_visible_release_request_metrics(summary, segment)
    record_visible_release_setcolumns_shape(summary, segment)
    record_visible_release_descriptor_window(summary, segment)
    record_visible_release_descriptor_contract(summary, segment)


def record_visible_release_request_metrics(summary: dict[str, Any], text: str) -> None:
    if text.startswith("visible_inbox_release_without_query_rows:"):
        text = text.split(":", 1)[1]
    shape = first_field(text, "release_request_shape") or "unknown"
    request_rops = first_field(text, "request_rops") or "unknown"
    input_index = first_field(text, "release_input_index") or "?"
    response_index = first_field(text, "release_response_index") or "?"
    release_rop_count = first_field(text, "release_rop_count") or "?"
    batch_rop_count = first_field(text, "release_batch_rop_count") or "?"
    already_released = first_field(text, "release_same_execute_already_released") or "?"
    summary["visible_release_request_shapes"][
        f"shape={shape};rops={request_rops};in={input_index};out={response_index};"
        f"release_rops={release_rop_count};batch_rops={batch_rop_count};"
        f"duplicate={already_released}"
    ] += 1

    query_position_seen = (
        first_field(text, "release_query_position_seen_before_release") or "?"
    )
    findrow_seen = first_field(text, "release_findrow_seen_before_release") or "?"
    query_rows_seen = first_field(text, "release_query_rows_seen_before_release") or "?"
    content_sync_seen = first_field(text, "release_content_sync_seen_before_release") or "?"
    live_handle_count = first_field(text, "release_live_handle_count_before") or "?"
    summary["visible_release_pre_release_states"][
        f"query_position={query_position_seen};findrow={findrow_seen};"
        f"query_rows={query_rows_seen};content_sync={content_sync_seen};"
        f"live_handles={live_handle_count}"
    ] += 1

    handle_slots = first_field(text, "release_handle_slots_before")
    if handle_slots:
        summary["visible_release_handle_slots"][handle_slots] += 1


def record_visible_release_setcolumns_shape(summary: dict[str, Any], text: str) -> None:
    if text.startswith("visible_inbox_release_without_query_rows:"):
        text = text.split(":", 1)[1]
    columns = first_field(text, "columns") or "unknown"
    sort = first_field(text, "sort") or "unknown"
    projection_kind = first_field(text, "default_view_projection_kind") or "unknown"
    descriptor_not_selected = first_field(text, "descriptor_columns_not_selected") or ""
    table_contract = first_field(text, "table_contract") or first_field(text, "contract") or ""
    descriptor_missing = first_field(text, "descriptor_columns_missing_from_table") or ""
    summary["visible_release_setcolumns_shapes"][
        f"columns={columns};sort={sort};projection={projection_kind};"
        f"descriptor_not_selected={descriptor_not_selected};"
        f"descriptor_missing_from_table={descriptor_missing};contract={table_contract}"
    ] += 1


def record_post_visible_release_terminal_event(
    summary: dict[str, Any], segment: str
) -> None:
    event_name = segment.split(":", 1)[0]
    if event_name == "visible_inbox_release_without_query_rows":
        return
    if event_name not in {
        "default_view_folder_open",
        "default_view_advertised",
        "getprops_folder",
        "hierarchy_table_open",
        "hierarchy_query_position",
        "hierarchy_query_rows",
    }:
        return
    contexts = summary.setdefault("post_visible_release_terminal_contexts", set())
    if segment in contexts:
        return
    contexts.add(segment)
    event_fields = segment.split(":", 1)[1] if ":" in segment else segment
    role = (
        first_field(event_fields, "role")
        or first_field(event_fields, "owner_role")
        or first_field(event_fields, "folder_role")
        or "unknown"
    )
    request_id = first_field(event_fields, "request_id") or "unknown"
    view_name = first_field(event_fields, "name") or first_field(
        event_fields, "selected_view_name"
    )
    detail_parts = [f"event={event_name}", f"role={role}"]
    if view_name:
        detail_parts.append(f"view={view_name}")
    if event_name == "hierarchy_query_rows":
        queried_position = first_field(event_fields, "queried_position") or "?"
        response_row_count = first_field(event_fields, "response_row_count") or "?"
        detail_parts.append(f"queried={queried_position}")
        detail_parts.append(f"rows={response_row_count}")
    elif event_name == "hierarchy_query_position":
        response_position = first_field(event_fields, "response_position") or "?"
        response_row_count = first_field(event_fields, "response_row_count") or "?"
        detail_parts.append(f"position={response_position}")
        detail_parts.append(f"rows={response_row_count}")
    detail_parts.append(f"request={request_id}")
    detail = ";".join(detail_parts)
    summary["post_visible_release_terminal_events"][detail] += 1
    summary["post_visible_release_terminal_tail"].append(detail)


def record_default_view_id_collision(summary: dict[str, Any], segment: str) -> None:
    if not segment.startswith("default_view_advertised:"):
        return
    fields = segment.split(":", 1)[1]
    view_id = first_field(fields, "view")
    owner_folder = first_field(fields, "owner_folder")
    if not view_id or not owner_folder:
        return
    owners = summary.setdefault("default_view_id_owners", defaultdict(set))[view_id]
    owners.add(owner_folder)
    if len(owners) < 2:
        return
    key = f"view={view_id};owners={','.join(sorted(owners))}"
    contexts = summary.setdefault("default_view_id_collision_contexts", set())
    if key in contexts:
        return
    contexts.add(key)
    summary["default_view_id_collisions"][key] += 1


def record_visible_release_classification(summary: dict[str, Any], text: str) -> None:
    if text.startswith("visible_inbox_release_without_query_rows:"):
        text = text.split(":", 1)[1]
    row_count = int_text_field(text, "row_count")
    defaulted = first_field(text, "defaulted")
    missing_descriptor = first_field(text, "selected_missing_descriptor_columns")
    descriptor_missing_from_table = first_field(text, "descriptor_columns_missing_from_table")
    descriptor_columns_not_selected = first_field(text, "descriptor_columns_not_selected")
    projection_kind = first_field(text, "default_view_projection_kind")
    table_sort_matches = first_field(text, "table_sort_matches_descriptor")
    descriptor_sort = first_field(text, "descriptor_sort_tag")
    table_sort = first_field(text, "table_primary_sort_tag")
    if projection_kind == "identity_probe_subset" or (
        row_count > 0
        and defaulted == ""
        and missing_descriptor == ""
        and descriptor_columns_not_selected
    ):
        key = "identity_probe_subset_before_query_rows"
    elif descriptor_missing_from_table and missing_descriptor == "" and row_count > 0:
        key = "descriptor_superset_client_subset_before_query_rows"
    elif descriptor_missing_from_table:
        key = "descriptor_table_mismatch_before_query_rows"
    elif row_count > 0 and defaulted == "" and missing_descriptor == "":
        if table_sort_matches == "true" or (
            descriptor_sort and table_sort and descriptor_sort == table_sort
        ):
            key = "valid_projection_complete_setcolumns_before_query_rows"
        else:
            key = "valid_projection_sort_mismatch_before_query_rows"
    elif defaulted or missing_descriptor:
        key = "incomplete_projection_before_query_rows"
    else:
        key = "empty_or_unknown_projection_before_query_rows"
    summary["visible_release_classifications"][key] += 1


def record_setcolumns_release_response(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    if field_text(fields, "request_rop_names") != "SetColumns,Release,Release,Release":
        return
    frames = field_text(fields, "response_rop_frames")
    if frames:
        summary["setcolumns_release_response_frames"][frames] += 1
    handle_table = field_text(fields, "output_handle_table_summary")
    if handle_table:
        summary["setcolumns_release_response_handle_tables"][handle_table] += 1
        summary["setcolumns_release_response_handle_classifications"][
            classify_setcolumns_release_response_handle_table(fields, handle_table)
        ] += 1


def classify_setcolumns_release_response_handle_table(
    fields: dict[str, Any], handle_table: str
) -> str:
    request_frames = field_text(fields, "request_rop_raw_frames")
    release_indexes = [
        int(match.group(1))
        for match in re.finditer(r"0x01@[^|]*:in=(\d+):", request_frames)
    ]
    if not release_indexes:
        if field_text(fields, "request_rop_names") == "SetColumns,Release,Release,Release":
            release_indexes = [0]
        else:
            return "release_input_slot_unknown"
    handles = parse_handle_table_summary(handle_table)
    if not handles:
        return "response_handle_table_unknown"
    saw_trimmed = False
    saw_invalidated = False
    for release_index in release_indexes:
        if release_index >= len(handles):
            saw_trimmed = True
            continue
        handle = handles[release_index].lower()
        if handle in {"0x00000000", "0xffffffff"}:
            saw_invalidated = True
            continue
        return "released_slot_reused_in_response_handle_table"
    if saw_invalidated:
        return "ms_oxcrops_release_invalidated_handle_table_entry"
    if saw_trimmed:
        return "ms_oxcrops_release_trimmed_unreferenced_handle_table_entry"
    return "release_input_slot_unknown"


def parse_handle_table_summary(summary: str) -> list[str]:
    match = re.search(r"(?:^|;)handles=([^;]+)", summary)
    if not match:
        return []
    return [part.strip() for part in match.group(1).split(",") if part.strip()]


def record_default_view_query_position_without_rows(
    summary: dict[str, Any], text: str
) -> None:
    event_name = text.split(":", 1)[0]
    if not event_name.endswith("_query_position_wire"):
        return
    event_fields = text.split(":", 1)[1] if ":" in text else text
    if first_field(event_fields, "query_rows_observed") != "false":
        return
    contexts = summary.setdefault("default_view_query_position_without_rows_contexts", set())
    if text in contexts:
        return
    contexts.add(text)
    role = first_field(event_fields, "role") or first_field(event_fields, "folder_role")
    if not role:
        if event_name.startswith("calendar_"):
            role = "calendar"
        elif event_name.startswith("visible_inbox_"):
            role = "inbox"
        else:
            role = event_name.removesuffix("_query_position_wire")
    next_step = first_field(event_fields, "next_expected_client_step") or ""
    key = f"role={role};next={next_step}"
    summary["default_view_query_position_without_rows"][key] += 1
    if role == "calendar" and "zero_duration_timed=true" in text:
        row_title = first_field(event_fields, "title") or "unknown"
        duration = first_field(event_fields, "duration_minutes") or "unknown"
        summary["calendar_zero_duration_timed_query_position_rows"][
            f"title={row_title};duration={duration};next={next_step}"
        ] += 1


def record_query_position_wire_fields(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    for key in (
        "visible_inbox_query_position_wire",
        "calendar_query_position_wire",
        "default_view_query_position_wire",
    ):
        value = fields.get(key)
        if isinstance(value, str):
            record_default_view_query_position_without_rows(summary, f"{key}:{value}")


def record_default_view_folder_open_without_rows(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    if field_text(fields, "default_view_folder_open_without_query_rows").lower() != "true":
        return
    context = field_text(fields, "last_default_view_folder_open_context")
    role = first_field(context, "role") or "unknown"
    folder_id = first_field(context, "folder") or field_text(
        fields, "last_default_view_folder_open_folder_id"
    )
    key = f"role={role};folder={folder_id or 'unknown'}"
    summary["default_view_folder_open_without_rows"][key] += 1


def record_post_calendar_query_position_named_property_probe(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    context = field_text(fields, "calendar_query_position_context")
    if not context:
        context = field_text(fields, "last_post_calendar_query_position_named_property_context")
    response_position = first_field(context, "response_position") or "unknown"
    response_row_count = first_field(context, "response_row_count") or "unknown"
    requested = (
        field_text(fields, "requested_named_property_count")
        or first_field(context, "requested")
        or "unknown"
    )
    missing = (
        field_text(fields, "pre_resolution_missing_named_property_count")
        or field_text(fields, "missing_named_property_count")
        or first_field(context, "pre_resolution_missing")
        or "unknown"
    )
    unresolved = (
        field_text(fields, "unresolved_returned_property_id_count")
        or first_field(context, "unresolved_returned")
        or "unknown"
    )
    source_summary = (
        field_text(fields, "returned_property_id_sources")
        or first_field(context, "property_id_sources")
        or "unknown"
    )
    key = (
        f"object={field_text(fields, 'object_kind') or 'unknown'};"
        f"requested={requested};"
        f"missing={missing};"
        f"unresolved={unresolved};"
        f"sources={source_summary};"
        f"calendar_position={response_position};calendar_rows={response_row_count}"
    )
    summary["post_calendar_query_position_named_property_probes"][key] += 1


def record_descriptor_gap(
    summary: dict[str, Any], text: str, fields: dict[str, Any] | None = None
) -> None:
    missing = first_field(text, "selected_missing_descriptor_columns")
    if not missing or missing in ("False", "none"):
        return
    associated = (
        field_text(fields or {}, "associated") or first_field(text, "associated") or ""
    )
    folder_role = (
        field_text(fields or {}, "folder_role")
        or first_field(text, "role")
        or first_field(text, "owner_role")
    )
    view_name = first_field(text, "selected_view_name") or first_field(text, "view_name")
    event_name = text.split(":", 1)[0]
    if associated.lower() == "true":
        table_kind = "associated"
    elif associated.lower() == "false" or event_name in (
        "outlook_default_view_setcolumns",
        "calendar_query_position_wire",
        "default_view_normal_table_open",
    ):
        table_kind = "visible"
    else:
        table_kind = "unknown"
    key = f"{table_kind};role={folder_role};view={view_name};missing={missing}"
    summary["descriptor_gap_windows"][key] += 1


DEFAULT_VIEW_IDENTITY_DESCRIPTOR_TAGS = {
    "0x67480014": "PidTagFolderId",
    "0x674a0014": "PidTagMid",
    "0x674d0014": "PidTagInstId",
    "0x674e0003": "PidTagInstanceNum",
}


def record_default_view_descriptor_identity_columns(
    summary: dict[str, Any], text: str
) -> None:
    if "descriptor_summary=" not in text:
        return
    role = first_field(text, "role") or first_field(text, "owner_role") or "unknown"
    view_name = first_field(text, "selected_view_name") or first_field(text, "view_name")
    if not view_name:
        return
    descriptor_summary = suffix_field(text, "descriptor_summary")
    if not descriptor_summary:
        return
    visible_tags = csv_field_values(
        first_field(descriptor_summary, "visible_column_tags") or ""
    )
    identity_tags = [
        f"{tag}:{DEFAULT_VIEW_IDENTITY_DESCRIPTOR_TAGS[tag]}"
        for tag in visible_tags
        if tag in DEFAULT_VIEW_IDENTITY_DESCRIPTOR_TAGS
    ]
    if not identity_tags:
        return
    summary["default_view_descriptor_identity_columns"][
        f"role={role};view={view_name};identity={','.join(identity_tags)}"
    ] += 1


def record_visible_release_descriptor_window(
    summary: dict[str, Any], segment: str
) -> None:
    marker = "descriptor_query_window="
    index = segment.find(marker)
    if index < 0:
        return
    window = segment[index + len(marker) :]
    key = (
        f"rows={first_field(window, 'total_rows') or ''};"
        f"position={first_field(window, 'position') or ''};"
        f"requested={first_field(window, 'requested') or ''};"
        f"sampled={first_field(window, 'sampled') or ''};"
        f"missing={first_field(window, 'selected_missing_descriptor_columns') or ''};"
        f"projection={first_field(window, 'descriptor_column_projection') or ''}"
    )
    sample = suffix_field(window, "sample_values")
    if sample:
        key = f"{key};sample={sample[:240]}"
    summary["visible_release_descriptor_windows"][key] += 1


EXPECTED_INBOX_COMPACT_DESCRIPTOR_VISIBLE_TAGS = [
    "0x00170003",
    "0x8503000b",
    "0x001a001e",
    "0x10900003",
    "0x0e1b000b",
    "0x0042001e",
    "0x0037001e",
    "0x0e060040",
    "0x0e080003",
    "0x9000101e",
]

FORBIDDEN_INBOX_COMPACT_DESCRIPTOR_TAGS = {
    "0x67480014": "identity_folder_id",
    "0x674a0014": "identity_mid",
    "0x674d0014": "identity_inst_id",
    "0x674e0003": "identity_instance_num",
    "0x8514000b": "old_outlook_common_8514_reminder",
    "0x001a001f": "unicode_message_class",
    "0x0e170003": "message_status_for_flag_status",
    "0x0042001f": "unicode_from",
    "0x0037001f": "unicode_subject",
    "0x12130003": "old_auxiliary_flags_for_size",
}


def record_visible_release_descriptor_contract(
    summary: dict[str, Any], segment: str
) -> None:
    if segment.startswith("visible_inbox_release_without_query_rows:"):
        text = segment.split(":", 1)[1]
    else:
        text = segment
    view_name = first_field(text, "selected_view_name") or first_field(text, "view_name")
    role = first_field(text, "role") or first_field(text, "owner_role")
    if view_name != "Compact" or role not in ("", None, "inbox"):
        return
    descriptor_summary = suffix_field(text, "descriptor_summary")
    if not descriptor_summary:
        return
    visible_tags = csv_field_values(
        first_field(descriptor_summary, "visible_column_tags") or ""
    )
    if not visible_tags:
        return
    missing_expected = [
        tag for tag in EXPECTED_INBOX_COMPACT_DESCRIPTOR_VISIBLE_TAGS if tag not in visible_tags
    ]
    forbidden = [
        f"{tag}:{FORBIDDEN_INBOX_COMPACT_DESCRIPTOR_TAGS[tag]}"
        for tag in visible_tags
        if tag in FORBIDDEN_INBOX_COMPACT_DESCRIPTOR_TAGS
    ]
    issue_parts = []
    if missing_expected:
        issue_parts.append(f"missing_expected={','.join(missing_expected)}")
    if forbidden:
        issue_parts.append(f"forbidden={','.join(forbidden)}")
    if not issue_parts:
        summary["visible_release_descriptor_contract_issues"][
            "role=inbox;view=Compact;contract=ms_oxocfg_ok"
        ] += 1
        return
    summary["visible_release_descriptor_contract_issues"][
        f"role=inbox;view=Compact;{';'.join(issue_parts)}"
    ] += 1


def csv_field_values(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip().lower() for part in value.split(",") if part.strip()]


def suffix_field(text: str, key: str) -> str | None:
    prefix = f"{key}="
    index = text.find(prefix)
    if index < 0:
        return None
    return text[index + len(prefix) :].split(">", 1)[0]


def first_field(text: str, key: str) -> str | None:
    prefix = f"{key}="
    for part in text.split(";"):
        part = part.strip()
        if part.startswith(prefix):
            return part[len(prefix) :].split(">", 1)[0]
    return None


def field_in_semicolon_text(text: str, key: str) -> str | None:
    return first_field(text, key)


def inspect_contract(
    summary: dict[str, Any], contract: str, fields: dict[str, Any] | None = None
) -> None:
    if not contract:
        return
    resolved_named_tags = summary.get("resolved_named_getprops_tags", set())
    zero_default_tags = {
        tag for key in ("zero_default_tags=", "zero_defaults=") for tag in tags_after(contract, key)
    }
    problem_tags = set(problem_tags_after(contract, "problem_tags="))
    for tag in unknown_named_tags(contract):
        if tag in resolved_named_tags:
            continue
        if tag in problem_tags:
            continue
        if tag in zero_default_tags:
            record_unknown_getprops_tag(
                summary, tag, contract, fields, "unknown-name-defaulted"
            )
        else:
            record_unknown_getprops_tag(summary, tag, contract, fields, "unknown-name")
    for tag in problem_tags:
        if tag:
            record_getprops_problem_tag(summary, tag, contract, fields)
    for tag in zero_default_tags:
        summary["zero_default_tags"][tag] += 1


def record_getprops_problem_tag(
    summary: dict[str, Any],
    tag: str,
    contract: str,
    fields: dict[str, Any] | None,
) -> None:
    request_id = field_text(fields or {}, "mapi_request_id") or first_field(
        contract, "request_id"
    )
    object_kind = field_text(fields or {}, "object_kind") or first_field(
        contract, "kind"
    )
    associated_config_class = field_text(fields or {}, "associated_config_class")
    umolk_problem = (
        object_kind == "associated_config"
        and (
            associated_config_class == UMOLK_USER_OPTIONS_CLASS
            or (request_id and request_id in summary["umolk_getprops_request_ids"])
        )
    )
    if umolk_problem:
        tag_counter = "umolk_problem_getprops_tags"
        context_counter = "umolk_problem_getprops_contexts"
    else:
        tag_counter = "problem_getprops_tags"
        context_counter = "problem_getprops_contexts"
    summary[tag_counter][tag] += 1
    role = first_field(contract, "role") or field_text(fields or {}, "folder_role")
    folder = first_field(contract, "folder") or field_text(fields or {}, "folder_id")
    problem = problem_detail_for_tag(contract, tag)
    context = (
        f"{tag};object={object_kind or 'unknown'};role={role or 'unknown'};"
        f"folder={folder or 'unknown'};request={request_id or 'unknown'}"
    )
    if associated_config_class and associated_config_class != "none":
        context = f"{context};class={associated_config_class}"
    if problem:
        context = f"{context};problem={problem}"
    summary[context_counter][context] += 1


def record_unknown_getprops_tag(
    summary: dict[str, Any],
    tag: str,
    contract: str,
    fields: dict[str, Any] | None,
    source: str,
) -> None:
    if source.endswith("-defaulted"):
        tag_counter = "unknown_defaulted_getprops_tags"
        context_counter = "unknown_defaulted_getprops_contexts"
    else:
        tag_counter = "unknown_getprops_tags"
        context_counter = "unknown_getprops_contexts"
    request_id = field_text(fields or {}, "mapi_request_id") or first_field(
        contract, "request_id"
    )
    object_kind = field_text(fields or {}, "object_kind") or first_field(
        contract, "kind"
    )
    role = first_field(contract, "role") or field_text(fields or {}, "folder_role")
    folder = first_field(contract, "folder") or field_text(fields or {}, "folder_id")
    response = (first_field(contract, "response") or "").rstrip(")")
    optional_associated_config_default = (
        source.endswith("-defaulted")
        and object_kind == "associated_config"
        and response == "0x00000000"
    )
    if optional_associated_config_default:
        tag_counter = "associated_config_optional_defaulted_getprops_tags"
        context_counter = "associated_config_optional_defaulted_getprops_contexts"
    summary[tag_counter][tag] += 1
    context = (
        f"{tag};object={object_kind or 'unknown'};role={role or 'unknown'};"
        f"folder={folder or 'unknown'};request={request_id or 'unknown'};"
        f"source={source}"
    )
    if response:
        context = f"{context};response={response}"
    summary[context_counter][context] += 1


def record_resolved_named_property_context(
    summary: dict[str, Any], fields: dict[str, Any]
) -> None:
    context = str(fields.get("named_property_context") or "")
    if not context:
        return
    for segment in context.split("|"):
        if (
            ("source=session" not in segment and "source=well_known" not in segment)
            or ("name=" not in segment and "lid=" not in segment)
        ):
            continue
        match = HEX_TAG_RE.search(segment)
        if match:
            summary["resolved_named_getprops_tags"].add(match.group(0).lower())


def unknown_named_tags(contract: str) -> list[str]:
    tags = tags_after(contract, "tags=")
    names = csv_field(contract, "names=")
    if not tags or not names:
        return []
    return [
        tag
        for tag, name in zip(tags, names)
        if name.strip().lower() == "unknown"
        and tag.lower() not in KNOWN_STATIC_GETPROPS_TAGS
    ]


def descriptor_gap_is_actionable(key: str) -> bool:
    if not key.startswith("visible;"):
        return False
    missing = first_field(key, "missing")
    if not missing:
        return False
    return any(
        tag.lower() not in KNOWN_BACKED_DESCRIPTOR_TAGS
        for tag in HEX_TAG_RE.findall(missing)
    )


def csv_field(text: str, key: str) -> list[str]:
    index = text.find(key)
    if index < 0:
        return []
    return text[index + len(key) :].split(";", 1)[0].split(")", 1)[0].split(",")


def tags_after(text: str, key: str) -> list[str]:
    index = text.find(key)
    if index < 0:
        return []
    value = text[index + len(key) :].split(";", 1)[0].split(")", 1)[0]
    return HEX_TAG_RE.findall(value)


def problem_tags_after(text: str, key: str) -> list[str]:
    index = text.find(key)
    if index < 0:
        return []
    value = text[index + len(key) :].split(";", 1)[0].split(")", 1)[0]
    tags = []
    for item in value.split(","):
        match = HEX_TAG_RE.match(item.strip())
        if match:
            tags.append(match.group(0))
    return tags


def problem_detail_for_tag(text: str, tag: str) -> str | None:
    index = text.find("problem_tags=")
    if index < 0:
        return None
    value = text[index + len("problem_tags=") :].split(";", 1)[0].split(")", 1)[0]
    tag_lower = tag.lower()
    for item in value.split(","):
        item = item.strip()
        if item.lower().startswith(tag_lower):
            return item
    return None


def print_counter(title: str, counter: Counter[str], limit: int = 12) -> None:
    print(title)
    if not counter:
        print("  none")
        return
    for key, count in counter.most_common(limit):
        print(f"  {key}: {count}")


def actionable_descriptor_gap_counts(counter: Counter[str]) -> Counter[str]:
    return Counter(
        {key: count for key, count in counter.items() if descriptor_gap_is_actionable(key)}
    )


def unknown_tag_class_counts(counter: Counter[str]) -> Counter[str]:
    classes: Counter[str] = Counter()
    for tag, count in counter.items():
        classes[classify_unknown_getprops_tag(tag)] += count
    return classes


def classify_unknown_getprops_tag(tag: str) -> str:
    match = HEX_TAG_RE.fullmatch(tag)
    if not match:
        return "malformed"
    value = int(tag, 16)
    if value == 0x65C6_0003:
        return "cfxics-unspecified-int32"
    property_id = value >> 16
    if property_id >= 0x8000:
        return "named-or-dynamic"
    if 0x6600 <= property_id <= 0x67FF:
        return "provider-defined-internal"
    if 0x6800 <= property_id <= 0x7BFF:
        return "outlook-or-store-private"
    return "unconfirmed-standard-range"


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
    print_counter(
        "RR SetColumns+Release response frames",
        rr["setcolumns_release_response_frames"],
        limit=8,
    )
    print_counter(
        "RR SetColumns+Release response previews",
        rr["setcolumns_release_response_previews"],
        limit=8,
    )
    print_counter(
        "RR SetColumns+Release response handle classifications",
        rr["setcolumns_release_response_handle_classifications"],
        limit=8,
    )

    sessions = rr["sessions"]
    if sessions:
        longest_session = max(sessions, key=lambda key: len(sessions[key]))
        print(f"Last ROPs in busiest session ({longest_session}):")
        for names in sessions[longest_session]:
            print(f"  {names}")
    session_event_tails = rr["session_event_tails"]
    if session_event_tails:
        busiest_event_session = max(
            session_event_tails, key=lambda key: len(session_event_tails[key])
        )
        print(f"Last trace events in busiest session ({busiest_event_session}):")
        for event_summary in session_event_tails[busiest_event_session]:
            print(f"  {event_summary}")
    emsmdb_session_event_tails = rr["emsmdb_session_event_tails"]
    if emsmdb_session_event_tails:
        busiest_emsmdb_session = max(
            emsmdb_session_event_tails,
            key=lambda key: len(emsmdb_session_event_tails[key]),
        )
        print(f"Last trace events in busiest EMSMDB session ({busiest_emsmdb_session}):")
        for event_summary in emsmdb_session_event_tails[busiest_emsmdb_session]:
            print(f"  {event_summary}")

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
        print_counter(
            "Visible Inbox QueryRows tracked",
            log["visible_inbox_query_rows"],
            limit=12,
        )
        print_counter(
            "Journal QueryRows response frames",
            log["query_rows_response_frames"],
            limit=32,
        )
        print_counter("Unknown GetProps tags", log["unknown_getprops_tags"], limit=20)
        print_counter(
            "Unknown GetProps tag classes",
            unknown_tag_class_counts(log["unknown_getprops_tags"]),
        )
        print_counter(
            "Unknown GetProps contexts",
            log["unknown_getprops_contexts"],
            limit=20,
        )
        print_counter("Problem GetProps tags", log["problem_getprops_tags"], limit=20)
        print_counter(
            "Problem GetProps contexts",
            log["problem_getprops_contexts"],
            limit=20,
        )
        print_counter(
            "UMOLK problem GetProps tags",
            log["umolk_problem_getprops_tags"],
            limit=20,
        )
        print_counter(
            "UMOLK problem GetProps contexts",
            log["umolk_problem_getprops_contexts"],
            limit=20,
        )
        print_counter(
            "Unknown defaulted GetProps tags",
            log["unknown_defaulted_getprops_tags"],
            limit=20,
        )
        print_counter(
            "Unknown defaulted GetProps contexts",
            log["unknown_defaulted_getprops_contexts"],
            limit=20,
        )
        print_counter(
            "Associated-config optional defaulted GetProps tags",
            log["associated_config_optional_defaulted_getprops_tags"],
            limit=20,
        )
        print_counter(
            "Associated-config optional defaulted GetProps contexts",
            log["associated_config_optional_defaulted_getprops_contexts"],
            limit=20,
        )
        print_counter("Zero-default tags", log["zero_default_tags"], limit=20)
        print_counter(
            "Actionable zero-default tags",
            actionable_zero_default_tag_counts(log["zero_default_tags"]),
            limit=20,
        )
        print_counter("Stale default-view owner states", log["stale_default_view_states"])
        print_counter("Descriptor gap windows", log["descriptor_gap_windows"], limit=12)
        print_counter(
            "Default-view descriptor identity columns",
            log["default_view_descriptor_identity_columns"],
            limit=12,
        )
        print_counter(
            "Actionable descriptor gap windows",
            actionable_descriptor_gap_counts(log["descriptor_gap_windows"]),
            limit=12,
        )
        print_counter(
            "Folder-local default view FAI visibility",
            log["folder_local_default_view_visibility"],
            limit=12,
        )
        print_counter("Hierarchy QueryRows windows", log["hierarchy_query_windows"], limit=12)
        print_counter(
            "Default-view folder open without QueryRows",
            log["default_view_folder_open_without_rows"],
            limit=12,
        )
        print_counter(
            "Default-view QueryPosition without QueryRows",
            log["default_view_query_position_without_rows"],
            limit=12,
        )
        print_counter(
            "Default-view ID collisions",
            log["default_view_id_collisions"],
            limit=8,
        )
        print_counter(
            "Calendar zero-duration timed rows at QueryPosition",
            log["calendar_zero_duration_timed_query_position_rows"],
            limit=12,
        )
        print_counter(
            "Post-Calendar QueryPosition named-property probes",
            log["post_calendar_query_position_named_property_probes"],
            limit=12,
        )
        print(f"Visible Inbox release-before-QueryRows events: {log['visible_release_without_query_rows']}")
        print_counter(
            "Visible Inbox release classifications",
            log["visible_release_classifications"],
            limit=8,
        )
        print_counter(
            "Visible Inbox release request shapes",
            log["visible_release_request_shapes"],
            limit=8,
        )
        print_counter(
            "Visible Inbox release SetColumns shapes",
            log["visible_release_setcolumns_shapes"],
            limit=8,
        )
        print_counter(
            "Visible Inbox release pre-release states",
            log["visible_release_pre_release_states"],
            limit=8,
        )
        print_counter(
            "Visible Inbox release handle slots",
            log["visible_release_handle_slots"],
            limit=8,
        )
        print_counter(
            "Journal SetColumns+Release response frames",
            log["setcolumns_release_response_frames"],
            limit=8,
        )
        print_counter(
            "Journal SetColumns+Release response handle tables",
            log["setcolumns_release_response_handle_tables"],
            limit=8,
        )
        print_counter(
            "Journal SetColumns+Release response handle classifications",
            log["setcolumns_release_response_handle_classifications"],
            limit=8,
        )
        print_counter(
            "Post-visible-release followups",
            log["post_visible_release_followups"],
            limit=8,
        )
        print_counter(
            "Post-visible-release terminal events",
            log["post_visible_release_terminal_events"],
            limit=12,
        )
        if log["post_visible_release_terminal_tail"]:
            print("Post-visible-release terminal tail:")
            for event in log["post_visible_release_terminal_tail"]:
                print(f"  {event}")
        print_counter("UMOLK dictionary shapes", log["umolk_dictionary_shapes"], limit=8)
        print_counter("UMOLK dictionary issues", log["umolk_dictionary_issues"], limit=8)
        print(
            "Post-visible-release hierarchy QueryPosition max: "
            f"{log['post_visible_release_hierarchy_query_position_max']}"
        )
        print_counter(
            "Visible Inbox release descriptor windows",
            log["visible_release_descriptor_windows"],
            limit=8,
        )
        print_counter(
            "Visible Inbox release descriptor contract issues",
            log["visible_release_descriptor_contract_issues"],
            limit=8,
        )
        print_counter(
            "Common-view descriptor GetProps",
            log["common_view_descriptor_getprops"],
            limit=12,
        )
        print_counter(
            "Common-view descriptor GetProps issues",
            log["common_view_descriptor_getprops_issues"],
            limit=12,
        )
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
    if log_path and actionable_issue_buckets(rr, log, log_path):
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
    aggregate_unknown_contexts: Counter[str] = Counter()
    aggregate_unknown_defaulted_tags: Counter[str] = Counter()
    aggregate_unknown_defaulted_contexts: Counter[str] = Counter()
    aggregate_problem_getprops_tags: Counter[str] = Counter()
    aggregate_problem_getprops_contexts: Counter[str] = Counter()
    aggregate_umolk_problem_getprops_tags: Counter[str] = Counter()
    aggregate_umolk_problem_getprops_contexts: Counter[str] = Counter()
    aggregate_associated_config_optional_defaulted_tags: Counter[str] = Counter()
    aggregate_associated_config_optional_defaulted_contexts: Counter[str] = Counter()
    aggregate_hierarchy_windows: Counter[str] = Counter()
    aggregate_visible_release_descriptor_windows: Counter[str] = Counter()
    aggregate_visible_release_descriptor_contract_issues: Counter[str] = Counter()
    aggregate_common_view_descriptor_getprops: Counter[str] = Counter()
    aggregate_common_view_descriptor_getprops_issues: Counter[str] = Counter()
    aggregate_visible_release_classifications: Counter[str] = Counter()
    aggregate_visible_release_request_shapes: Counter[str] = Counter()
    aggregate_visible_release_setcolumns_shapes: Counter[str] = Counter()
    aggregate_visible_release_pre_release_states: Counter[str] = Counter()
    aggregate_visible_release_handle_slots: Counter[str] = Counter()
    aggregate_setcolumns_release_response_frames: Counter[str] = Counter()
    aggregate_rr_setcolumns_release_response_frames: Counter[str] = Counter()
    aggregate_setcolumns_release_response_handle_classifications: Counter[str] = Counter()
    aggregate_rr_setcolumns_release_response_handle_classifications: Counter[str] = Counter()
    aggregate_post_visible_release_followups: Counter[str] = Counter()
    aggregate_post_visible_release_terminal_events: Counter[str] = Counter()
    aggregate_umolk_dictionary_shapes: Counter[str] = Counter()
    aggregate_umolk_dictionary_issues: Counter[str] = Counter()
    aggregate_default_view_folder_open_without_rows: Counter[str] = Counter()
    aggregate_default_view_query_position_without_rows: Counter[str] = Counter()
    aggregate_default_view_id_collisions: Counter[str] = Counter()
    aggregate_calendar_zero_duration_timed_query_position_rows: Counter[str] = Counter()
    aggregate_post_calendar_query_position_named_property_probes: Counter[str] = Counter()
    aggregate_descriptor_gap_windows: Counter[str] = Counter()
    aggregate_default_view_descriptor_identity_columns: Counter[str] = Counter()
    aggregate_nonzero_response_codes: Counter[str] = Counter()
    current_missing_gates: Counter[str] = Counter()
    current_stalls: Counter[str] = Counter()
    current_unknown_tags: Counter[str] = Counter()
    current_unknown_contexts: Counter[str] = Counter()
    current_unknown_defaulted_tags: Counter[str] = Counter()
    current_unknown_defaulted_contexts: Counter[str] = Counter()
    current_problem_getprops_tags: Counter[str] = Counter()
    current_problem_getprops_contexts: Counter[str] = Counter()
    current_umolk_problem_getprops_tags: Counter[str] = Counter()
    current_umolk_problem_getprops_contexts: Counter[str] = Counter()
    current_associated_config_optional_defaulted_tags: Counter[str] = Counter()
    current_associated_config_optional_defaulted_contexts: Counter[str] = Counter()
    current_descriptor_gap_windows: Counter[str] = Counter()
    current_visible_release_descriptor_windows: Counter[str] = Counter()
    current_visible_release_descriptor_contract_issues: Counter[str] = Counter()
    current_common_view_descriptor_getprops: Counter[str] = Counter()
    current_common_view_descriptor_getprops_issues: Counter[str] = Counter()
    current_visible_release_classifications: Counter[str] = Counter()
    current_visible_release_request_shapes: Counter[str] = Counter()
    current_visible_release_setcolumns_shapes: Counter[str] = Counter()
    current_visible_release_pre_release_states: Counter[str] = Counter()
    current_visible_release_handle_slots: Counter[str] = Counter()
    current_setcolumns_release_response_frames: Counter[str] = Counter()
    current_rr_setcolumns_release_response_frames: Counter[str] = Counter()
    current_setcolumns_release_response_handle_classifications: Counter[str] = Counter()
    current_rr_setcolumns_release_response_handle_classifications: Counter[str] = Counter()
    current_post_visible_release_followups: Counter[str] = Counter()
    current_post_visible_release_terminal_events: Counter[str] = Counter()
    current_umolk_dictionary_shapes: Counter[str] = Counter()
    current_umolk_dictionary_issues: Counter[str] = Counter()
    current_default_view_folder_open_without_rows: Counter[str] = Counter()
    current_default_view_query_position_without_rows: Counter[str] = Counter()
    current_default_view_id_collisions: Counter[str] = Counter()
    current_calendar_zero_duration_timed_query_position_rows: Counter[str] = Counter()
    current_post_calendar_query_position_named_property_probes: Counter[str] = Counter()
    current_nonzero_response_codes: Counter[str] = Counter()
    current_default_view_descriptor_identity_columns: Counter[str] = Counter()
    build_issue_counts: Counter[tuple[str, str]] = Counter()
    current_issue_counts: Counter[tuple[str, str]] = Counter()
    actionable_runs = 0
    current_run_count = 0

    print(
        "run,matched_log,build_commit,build_dirty,build_scope,rr_events,nonzero_mapi,parse_errors,missing_gate,"
        "visible_release_before_query,default_view_folder_open_without_rows,"
        "default_view_query_position_without_rows,post_calendar_named_property_probes,"
        "raw_umolk,stale_default_view,verdict"
    )
    for trace_dir in runs:
        log_path = matching_log_for_run(trace_dir.name, logs_by_stamp)
        rr = summarize_rr(trace_dir)
        log = summarize_log(log_path)
        verdict = verdict_for_summary(rr, log, log_path)
        if "ETL/client crash data" not in verdict:
            actionable_runs += 1
        build_scope = build_scope_for(
            log["build"].get("git_commit"),
            log["build"].get("git_dirty"),
            current_build,
        )
        is_current_build = build_scope.startswith("current-")
        aggregate_missing_gates.update(log["startup_missing_gates"])
        aggregate_stalls.update(log["stall_warnings"])
        aggregate_sequences.update(log["sequence_counts"])
        aggregate_unknown_tags.update(log["unknown_getprops_tags"])
        aggregate_unknown_contexts.update(log["unknown_getprops_contexts"])
        aggregate_unknown_defaulted_tags.update(log["unknown_defaulted_getprops_tags"])
        aggregate_unknown_defaulted_contexts.update(
            log["unknown_defaulted_getprops_contexts"]
        )
        aggregate_problem_getprops_tags.update(log["problem_getprops_tags"])
        aggregate_problem_getprops_contexts.update(log["problem_getprops_contexts"])
        aggregate_umolk_problem_getprops_tags.update(log["umolk_problem_getprops_tags"])
        aggregate_umolk_problem_getprops_contexts.update(
            log["umolk_problem_getprops_contexts"]
        )
        aggregate_associated_config_optional_defaulted_tags.update(
            log["associated_config_optional_defaulted_getprops_tags"]
        )
        aggregate_associated_config_optional_defaulted_contexts.update(
            log["associated_config_optional_defaulted_getprops_contexts"]
        )
        aggregate_hierarchy_windows.update(log["hierarchy_query_windows"])
        aggregate_descriptor_gap_windows.update(log["descriptor_gap_windows"])
        aggregate_default_view_descriptor_identity_columns.update(
            log["default_view_descriptor_identity_columns"]
        )
        aggregate_post_visible_release_followups.update(
            log["post_visible_release_followups"]
        )
        aggregate_post_visible_release_terminal_events.update(
            log["post_visible_release_terminal_events"]
        )
        aggregate_umolk_dictionary_shapes.update(log["umolk_dictionary_shapes"])
        aggregate_umolk_dictionary_issues.update(log["umolk_dictionary_issues"])
        aggregate_default_view_folder_open_without_rows.update(
            log["default_view_folder_open_without_rows"]
        )
        aggregate_default_view_query_position_without_rows.update(
            log["default_view_query_position_without_rows"]
        )
        aggregate_default_view_id_collisions.update(log["default_view_id_collisions"])
        aggregate_calendar_zero_duration_timed_query_position_rows.update(
            log["calendar_zero_duration_timed_query_position_rows"]
        )
        aggregate_post_calendar_query_position_named_property_probes.update(
            log["post_calendar_query_position_named_property_probes"]
        )
        aggregate_visible_release_descriptor_windows.update(
            log["visible_release_descriptor_windows"]
        )
        aggregate_visible_release_descriptor_contract_issues.update(
            log["visible_release_descriptor_contract_issues"]
        )
        aggregate_common_view_descriptor_getprops.update(
            log["common_view_descriptor_getprops"]
        )
        aggregate_common_view_descriptor_getprops_issues.update(
            log["common_view_descriptor_getprops_issues"]
        )
        aggregate_visible_release_classifications.update(
            log["visible_release_classifications"]
        )
        aggregate_visible_release_request_shapes.update(
            log["visible_release_request_shapes"]
        )
        aggregate_visible_release_setcolumns_shapes.update(
            log["visible_release_setcolumns_shapes"]
        )
        aggregate_visible_release_pre_release_states.update(
            log["visible_release_pre_release_states"]
        )
        aggregate_visible_release_handle_slots.update(
            log["visible_release_handle_slots"]
        )
        aggregate_setcolumns_release_response_frames.update(
            log["setcolumns_release_response_frames"]
        )
        aggregate_rr_setcolumns_release_response_frames.update(
            rr["setcolumns_release_response_frames"]
        )
        aggregate_setcolumns_release_response_handle_classifications.update(
            log["setcolumns_release_response_handle_classifications"]
        )
        aggregate_rr_setcolumns_release_response_handle_classifications.update(
            rr["setcolumns_release_response_handle_classifications"]
        )
        aggregate_nonzero_response_codes.update(rr["nonzero_response_codes"])
        build_commit = str(log["build"].get("git_commit", "unknown"))
        build_dirty = format_build_dirty(log["build"].get("git_dirty"))
        for issue in issue_buckets(rr, log, log_path):
            build_issue_counts[(f"{build_commit}/{build_dirty}", issue)] += 1
            if is_current_build:
                current_issue_counts[(f"{build_commit}/{build_dirty}", issue)] += 1
        if is_current_build:
            current_run_count += 1
            current_missing_gates.update(log["startup_missing_gates"])
            current_stalls.update(log["stall_warnings"])
            current_unknown_tags.update(log["unknown_getprops_tags"])
            current_unknown_contexts.update(log["unknown_getprops_contexts"])
            current_unknown_defaulted_tags.update(log["unknown_defaulted_getprops_tags"])
            current_unknown_defaulted_contexts.update(
                log["unknown_defaulted_getprops_contexts"]
            )
            current_problem_getprops_tags.update(log["problem_getprops_tags"])
            current_problem_getprops_contexts.update(log["problem_getprops_contexts"])
            current_umolk_problem_getprops_tags.update(log["umolk_problem_getprops_tags"])
            current_umolk_problem_getprops_contexts.update(
                log["umolk_problem_getprops_contexts"]
            )
            current_associated_config_optional_defaulted_tags.update(
                log["associated_config_optional_defaulted_getprops_tags"]
            )
            current_associated_config_optional_defaulted_contexts.update(
                log["associated_config_optional_defaulted_getprops_contexts"]
            )
            current_descriptor_gap_windows.update(log["descriptor_gap_windows"])
            current_default_view_descriptor_identity_columns.update(
                log["default_view_descriptor_identity_columns"]
            )
            current_post_visible_release_followups.update(
                log["post_visible_release_followups"]
            )
            current_post_visible_release_terminal_events.update(
                log["post_visible_release_terminal_events"]
            )
            current_umolk_dictionary_shapes.update(log["umolk_dictionary_shapes"])
            current_umolk_dictionary_issues.update(log["umolk_dictionary_issues"])
            current_default_view_folder_open_without_rows.update(
                log["default_view_folder_open_without_rows"]
            )
            current_default_view_query_position_without_rows.update(
                log["default_view_query_position_without_rows"]
            )
            current_default_view_id_collisions.update(log["default_view_id_collisions"])
            current_calendar_zero_duration_timed_query_position_rows.update(
                log["calendar_zero_duration_timed_query_position_rows"]
            )
            current_post_calendar_query_position_named_property_probes.update(
                log["post_calendar_query_position_named_property_probes"]
            )
            current_visible_release_descriptor_windows.update(
                log["visible_release_descriptor_windows"]
            )
            current_visible_release_descriptor_contract_issues.update(
                log["visible_release_descriptor_contract_issues"]
            )
            current_common_view_descriptor_getprops.update(
                log["common_view_descriptor_getprops"]
            )
            current_common_view_descriptor_getprops_issues.update(
                log["common_view_descriptor_getprops_issues"]
            )
            current_visible_release_classifications.update(
                log["visible_release_classifications"]
            )
            current_visible_release_request_shapes.update(
                log["visible_release_request_shapes"]
            )
            current_visible_release_setcolumns_shapes.update(
                log["visible_release_setcolumns_shapes"]
            )
            current_visible_release_pre_release_states.update(
                log["visible_release_pre_release_states"]
            )
            current_visible_release_handle_slots.update(
                log["visible_release_handle_slots"]
            )
            current_setcolumns_release_response_frames.update(
                log["setcolumns_release_response_frames"]
            )
            current_rr_setcolumns_release_response_frames.update(
                rr["setcolumns_release_response_frames"]
            )
            current_setcolumns_release_response_handle_classifications.update(
                log["setcolumns_release_response_handle_classifications"]
            )
            current_rr_setcolumns_release_response_handle_classifications.update(
                rr["setcolumns_release_response_handle_classifications"]
            )
            current_nonzero_response_codes.update(rr["nonzero_response_codes"])
        missing_gate = (
            log["startup_missing_gates"].most_common(1)[0][0]
            if log["startup_missing_gates"]
            else "none"
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
                    str(sum(log["default_view_folder_open_without_rows"].values())),
                    str(sum(log["default_view_query_position_without_rows"].values())),
                    str(sum(log["post_calendar_query_position_named_property_probes"].values())),
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
    print_counter(
        "Aggregate unknown GetProps tag classes",
        unknown_tag_class_counts(aggregate_unknown_tags),
    )
    print_counter(
        "Aggregate unknown GetProps contexts",
        aggregate_unknown_contexts,
        limit=20,
    )
    print_counter(
        "Aggregate problem GetProps tags",
        aggregate_problem_getprops_tags,
        limit=20,
    )
    print_counter(
        "Aggregate problem GetProps contexts",
        aggregate_problem_getprops_contexts,
        limit=20,
    )
    print_counter(
        "Aggregate UMOLK problem GetProps tags",
        aggregate_umolk_problem_getprops_tags,
        limit=20,
    )
    print_counter(
        "Aggregate UMOLK problem GetProps contexts",
        aggregate_umolk_problem_getprops_contexts,
        limit=20,
    )
    print_counter(
        "Aggregate unknown defaulted GetProps tags",
        aggregate_unknown_defaulted_tags,
        limit=20,
    )
    print_counter(
        "Aggregate unknown defaulted GetProps contexts",
        aggregate_unknown_defaulted_contexts,
        limit=20,
    )
    print_counter(
        "Aggregate associated-config optional defaulted GetProps tags",
        aggregate_associated_config_optional_defaulted_tags,
        limit=20,
    )
    print_counter(
        "Aggregate associated-config optional defaulted GetProps contexts",
        aggregate_associated_config_optional_defaulted_contexts,
        limit=20,
    )
    print_counter(
        "Aggregate descriptor gap windows",
        aggregate_descriptor_gap_windows,
        limit=20,
    )
    print_counter(
        "Aggregate actionable descriptor gap windows",
        actionable_descriptor_gap_counts(aggregate_descriptor_gap_windows),
        limit=20,
    )
    print_counter(
        "Aggregate default-view descriptor identity columns",
        aggregate_default_view_descriptor_identity_columns,
        limit=20,
    )
    print_counter("Aggregate hierarchy QueryRows windows", aggregate_hierarchy_windows, limit=20)
    print_counter(
        "Aggregate post-visible-release followups",
        aggregate_post_visible_release_followups,
        limit=20,
    )
    print_counter(
        "Aggregate post-visible-release terminal events",
        aggregate_post_visible_release_terminal_events,
        limit=20,
    )
    print_counter(
        "Aggregate UMOLK dictionary shapes",
        aggregate_umolk_dictionary_shapes,
        limit=20,
    )
    print_counter(
        "Aggregate UMOLK dictionary issues",
        aggregate_umolk_dictionary_issues,
        limit=20,
    )
    print_counter(
        "Aggregate default-view folder open without QueryRows",
        aggregate_default_view_folder_open_without_rows,
        limit=20,
    )
    print_counter(
        "Aggregate default-view QueryPosition without QueryRows",
        aggregate_default_view_query_position_without_rows,
        limit=20,
    )
    print_counter(
        "Aggregate default-view ID collisions",
        aggregate_default_view_id_collisions,
        limit=20,
    )
    print_counter(
        "Aggregate Calendar zero-duration timed rows at QueryPosition",
        aggregate_calendar_zero_duration_timed_query_position_rows,
        limit=20,
    )
    print_counter(
        "Aggregate post-Calendar QueryPosition named-property probes",
        aggregate_post_calendar_query_position_named_property_probes,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release descriptor windows",
        aggregate_visible_release_descriptor_windows,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release descriptor contract issues",
        aggregate_visible_release_descriptor_contract_issues,
        limit=20,
    )
    print_counter(
        "Aggregate common-view descriptor GetProps",
        aggregate_common_view_descriptor_getprops,
        limit=20,
    )
    print_counter(
        "Aggregate common-view descriptor GetProps issues",
        aggregate_common_view_descriptor_getprops_issues,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release classifications",
        aggregate_visible_release_classifications,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release request shapes",
        aggregate_visible_release_request_shapes,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release SetColumns shapes",
        aggregate_visible_release_setcolumns_shapes,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release pre-release states",
        aggregate_visible_release_pre_release_states,
        limit=20,
    )
    print_counter(
        "Aggregate visible Inbox release handle slots",
        aggregate_visible_release_handle_slots,
        limit=20,
    )
    print_counter(
        "Aggregate Journal SetColumns+Release response frames",
        aggregate_setcolumns_release_response_frames,
        limit=20,
    )
    print_counter(
        "Aggregate RR SetColumns+Release response frames",
        aggregate_rr_setcolumns_release_response_frames,
        limit=20,
    )
    print_counter(
        "Aggregate Journal SetColumns+Release response handle classifications",
        aggregate_setcolumns_release_response_handle_classifications,
        limit=20,
    )
    print_counter(
        "Aggregate RR SetColumns+Release response handle classifications",
        aggregate_rr_setcolumns_release_response_handle_classifications,
        limit=20,
    )
    if current_build:
        print(f"Current-build runs matched: {current_run_count}")
        if current_run_count == 0:
            print(
                "Current-build warning: no Outlook trace/log pair matched the "
                f"requested build prefix {current_build}"
            )
        print_counter(
            "Current-build non-zero MAPI response codes",
            current_nonzero_response_codes,
        )
        print_counter("Current-build startup first missing gates", current_missing_gates)
        print_counter("Current-build execute stall names", current_stalls)
        print_counter("Current-build unknown GetProps tags", current_unknown_tags, limit=20)
        print_counter(
            "Current-build unknown GetProps tag classes",
            unknown_tag_class_counts(current_unknown_tags),
        )
        print_counter(
            "Current-build unknown GetProps contexts",
            current_unknown_contexts,
            limit=20,
        )
        print_counter(
            "Current-build problem GetProps tags",
            current_problem_getprops_tags,
            limit=20,
        )
        print_counter(
            "Current-build problem GetProps contexts",
            current_problem_getprops_contexts,
            limit=20,
        )
        print_counter(
            "Current-build UMOLK problem GetProps tags",
            current_umolk_problem_getprops_tags,
            limit=20,
        )
        print_counter(
            "Current-build UMOLK problem GetProps contexts",
            current_umolk_problem_getprops_contexts,
            limit=20,
        )
        print_counter(
            "Current-build unknown defaulted GetProps tags",
            current_unknown_defaulted_tags,
            limit=20,
        )
        print_counter(
            "Current-build unknown defaulted GetProps contexts",
            current_unknown_defaulted_contexts,
            limit=20,
        )
        print_counter(
            "Current-build associated-config optional defaulted GetProps tags",
            current_associated_config_optional_defaulted_tags,
            limit=20,
        )
        print_counter(
            "Current-build associated-config optional defaulted GetProps contexts",
            current_associated_config_optional_defaulted_contexts,
            limit=20,
        )
        print_counter(
            "Current-build descriptor gap windows",
            current_descriptor_gap_windows,
            limit=20,
        )
        print_counter(
            "Current-build actionable descriptor gap windows",
            actionable_descriptor_gap_counts(current_descriptor_gap_windows),
            limit=20,
        )
        print_counter(
            "Current-build default-view descriptor identity columns",
            current_default_view_descriptor_identity_columns,
            limit=20,
        )
        print_counter(
            "Current-build post-visible-release followups",
            current_post_visible_release_followups,
            limit=20,
        )
        print_counter(
            "Current-build post-visible-release terminal events",
            current_post_visible_release_terminal_events,
            limit=20,
        )
        print_counter(
            "Current-build UMOLK dictionary shapes",
            current_umolk_dictionary_shapes,
            limit=20,
        )
        print_counter(
            "Current-build UMOLK dictionary issues",
            current_umolk_dictionary_issues,
            limit=20,
        )
        print_counter(
            "Current-build default-view folder open without QueryRows",
            current_default_view_folder_open_without_rows,
            limit=20,
        )
        print_counter(
            "Current-build default-view QueryPosition without QueryRows",
            current_default_view_query_position_without_rows,
            limit=20,
        )
        print_counter(
            "Current-build default-view ID collisions",
            current_default_view_id_collisions,
            limit=20,
        )
        print_counter(
            "Current-build Calendar zero-duration timed rows at QueryPosition",
            current_calendar_zero_duration_timed_query_position_rows,
            limit=20,
        )
        print_counter(
            "Current-build post-Calendar QueryPosition named-property probes",
            current_post_calendar_query_position_named_property_probes,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release descriptor windows",
            current_visible_release_descriptor_windows,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release descriptor contract issues",
            current_visible_release_descriptor_contract_issues,
            limit=20,
        )
        print_counter(
            "Current-build common-view descriptor GetProps",
            current_common_view_descriptor_getprops,
            limit=20,
        )
        print_counter(
            "Current-build common-view descriptor GetProps issues",
            current_common_view_descriptor_getprops_issues,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release classifications",
            current_visible_release_classifications,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release request shapes",
            current_visible_release_request_shapes,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release SetColumns shapes",
            current_visible_release_setcolumns_shapes,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release pre-release states",
            current_visible_release_pre_release_states,
            limit=20,
        )
        print_counter(
            "Current-build visible Inbox release handle slots",
            current_visible_release_handle_slots,
            limit=20,
        )
        print_counter(
            "Current-build Journal SetColumns+Release response frames",
            current_setcolumns_release_response_frames,
            limit=20,
        )
        print_counter(
            "Current-build RR SetColumns+Release response frames",
            current_rr_setcolumns_release_response_frames,
            limit=20,
        )
        print_counter(
            "Current-build Journal SetColumns+Release response handle classifications",
            current_setcolumns_release_response_handle_classifications,
            limit=20,
        )
        print_counter(
            "Current-build RR SetColumns+Release response handle classifications",
            current_rr_setcolumns_release_response_handle_classifications,
            limit=20,
        )
        print_build_issue_counts(current_issue_counts, "Current-build issue buckets")
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
    if visible_release_needs_action(log):
        issues.append("visible_inbox_release_before_query_rows")
    if log.get("visible_release_descriptor_contract_issues"):
        for name, _count in log[
            "visible_release_descriptor_contract_issues"
        ].most_common(2):
            if visible_release_descriptor_contract_issue_is_actionable(name):
                issues.append(f"visible_inbox_descriptor_contract:{name}")
    if log.get("common_view_descriptor_getprops_issues"):
        for name, _count in log["common_view_descriptor_getprops_issues"].most_common(2):
            issues.append(f"common_view_descriptor_getprops:{name}")
    if log.get("problem_getprops_tags"):
        for name, _count in stable_counter_items(log["problem_getprops_tags"], 2):
            issues.append(f"problem_getprops:{name}")
    if log.get("umolk_problem_getprops_tags"):
        for name, _count in problem_getprops_property_type_counts(
            log["umolk_problem_getprops_tags"]
        )[:4]:
            issues.append(f"umolk_problem_getprops_type:{name}")
    if log.get("visible_release_classifications"):
        for name, _count in log["visible_release_classifications"].most_common(2):
            if visible_release_classification_is_actionable(name):
                issues.append(f"visible_inbox_release_classification:{name}")
    if log.get("setcolumns_release_response_handle_classifications"):
        for name, _count in log[
            "setcolumns_release_response_handle_classifications"
        ].most_common(2):
            if setcolumns_release_response_handle_classification_is_actionable(name):
                issues.append(f"setcolumns_release_response_handle:{name}")
    if rr.get("setcolumns_release_response_handle_classifications"):
        for name, _count in rr[
            "setcolumns_release_response_handle_classifications"
        ].most_common(2):
            if setcolumns_release_response_handle_classification_is_actionable(name):
                issues.append(f"rr_setcolumns_release_response_handle:{name}")
    if log.get("post_visible_release_followups"):
        for name, _count in log["post_visible_release_followups"].most_common(2):
            if post_visible_release_followup_is_actionable(name):
                issues.append(f"post_visible_release:{name}")
    if log.get("umolk_dictionary_issues"):
        for name, _count in log["umolk_dictionary_issues"].most_common(2):
            issues.append(f"umolk_dictionary:{name}")
    if log.get("default_view_folder_open_without_rows"):
        for name, _count in log["default_view_folder_open_without_rows"].most_common(2):
            issues.append(f"default_view_folder_open_without_rows:{name}")
    if log.get("default_view_query_position_without_rows"):
        for name, _count in log["default_view_query_position_without_rows"].most_common(2):
            issues.append(f"default_view_query_position_without_rows:{name}")
    if log.get("default_view_id_collisions"):
        for name, _count in log["default_view_id_collisions"].most_common(2):
            issues.append(f"default_view_id_collision:{name}")
    if log.get("zero_default_tags"):
        for name, _count in actionable_zero_default_tag_counts(
            log["zero_default_tags"]
        ).most_common(2):
            issues.append(f"zero_default:{name}")
    if log.get("calendar_zero_duration_timed_query_position_rows"):
        issues.append("calendar_zero_duration_timed_query_position_row")
    if log.get("post_calendar_query_position_named_property_probes"):
        for name, _count in log[
            "post_calendar_query_position_named_property_probes"
        ].most_common(2):
            issues.append(f"post_calendar_query_position_named_property_probe:{name}")
    if log["raw_umolk_placeholder"]:
        issues.append("raw_umolk_placeholder")
    if log["stale_default_view_states"]:
        issues.append("stale_default_view_state")
    if any(descriptor_gap_is_actionable(key) for key in log.get("descriptor_gap_windows", {})):
        issues.append("visible_descriptor_gap")
    if any(
        "present=false" in key
        for key in log.get("folder_local_default_view_visibility", {})
    ):
        issues.append("folder_local_default_view_missing_from_fai")
    if log["stall_warnings"]:
        for name, _count in log["stall_warnings"].most_common(2):
            issues.append(f"stall:{name}")
    if log_path and log["startup_missing_gates"]:
        gate = log["startup_missing_gates"].most_common(1)[0][0]
        if not (
            gate == "normal_inbox_visible_row_observed"
            and log.get("visible_inbox_query_rows")
        ):
            issues.append(f"missing_gate:{gate}")
    issues = suppress_symptom_only_issues(issues)
    if not issues:
        issues.append("no_server_issue_detected")
    return issues


def suppress_symptom_only_issues(issues: list[str]) -> list[str]:
    concrete = [
        issue
        for issue in issues
        if not (issue.startswith("stall:") or issue.startswith("missing_gate:"))
    ]
    if concrete:
        return concrete
    return issues


def stable_counter_items(counter: Counter[str], limit: int) -> list[tuple[str, int]]:
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]


def problem_getprops_property_type_counts(counter: Counter[str]) -> list[tuple[str, int]]:
    type_counts: Counter[str] = Counter()
    for tag, count in counter.items():
        match = HEX_TAG_RE.fullmatch(tag)
        if not match:
            type_counts["malformed"] += count
            continue
        type_counts[f"0x{int(tag, 16) & 0xFFFF:04x}"] += count
    return stable_counter_items(type_counts, len(type_counts))


def actionable_issue_buckets(
    rr: dict[str, Any], log: dict[str, Any], log_path: Path | None
) -> list[str]:
    return [
        issue
        for issue in issue_buckets(rr, log, log_path)
        if issue != "no_server_issue_detected"
        and not (issue.startswith("stall:") or issue.startswith("missing_gate:"))
    ]


def setcolumns_release_response_handle_classification_is_actionable(name: str) -> bool:
    return name not in {
        "released_slot_invalidated_in_response_handle_table",
        "released_slot_trimmed_from_response_handle_table",
        "ms_oxcrops_release_invalidated_handle_table_entry",
        "ms_oxcrops_release_trimmed_unreferenced_handle_table_entry",
    }


def visible_release_needs_action(log: dict[str, Any]) -> bool:
    if not log.get("visible_release_without_query_rows"):
        return False
    if any(
        visible_release_descriptor_contract_issue_is_actionable(name)
        for name in log.get("visible_release_descriptor_contract_issues", {})
    ):
        return True
    classifications = log.get("visible_release_classifications")
    if not classifications:
        return True
    return any(visible_release_classification_is_actionable(name) for name in classifications)


def post_visible_release_followup_is_actionable(name: str) -> bool:
    return name in {"create_save_batch_after_visible_release"}


def visible_release_classification_is_actionable(name: str) -> bool:
    return name not in {
        "empty_or_unknown_projection_before_query_rows",
        "valid_projection_complete_setcolumns_before_query_rows",
    }


def visible_release_descriptor_contract_issue_is_actionable(name: str) -> bool:
    return "contract=ms_oxocfg_ok" not in name


def actionable_zero_default_tag_counts(counter: Counter[str]) -> Counter[str]:
    return Counter(
        {
            ACTIONABLE_ZERO_DEFAULT_TAGS[tag]: count
            for tag, count in counter.items()
            if tag in ACTIONABLE_ZERO_DEFAULT_TAGS
        }
    )


def print_build_issue_counts(
    counter: Counter[tuple[str, str]], title: str = "Issue buckets by build"
) -> None:
    print(title)
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


def trace_dir_for_log(trace_dir: Path, log_path: Path | None) -> Path:
    if not log_path:
        return trace_dir
    if trace_jsonl_paths(trace_dir):
        return trace_dir
    match = RUN_STAMP_RE.search(log_path.stem)
    if not match:
        return trace_dir
    direct_child = trace_dir / match.group(1)
    if trace_jsonl_paths(direct_child):
        return direct_child
    log_stamp = parse_stamp(match.group(1))
    if log_stamp is None:
        return trace_dir
    best: tuple[float, Path] | None = None
    for child in trace_dir.iterdir() if trace_dir.exists() else []:
        if not child.is_dir():
            continue
        run_stamp = parse_stamp(child.name)
        if run_stamp is None or run_stamp.date() != log_stamp.date():
            continue
        if not trace_jsonl_paths(child):
            continue
        delta_seconds = abs((run_stamp - log_stamp).total_seconds())
        if delta_seconds <= 180 and (best is None or delta_seconds < best[0]):
            best = (delta_seconds, child)
    return best[1] if best else trace_dir


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
    print_single_summary(trace_dir_for_log(args.trace_dir, args.log), args.log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
