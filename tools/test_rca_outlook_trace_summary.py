import importlib.util
import io
import json
import shutil
import unittest
from collections import Counter, defaultdict, deque
from contextlib import redirect_stdout
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("rca_outlook_trace_summary.py")
SPEC = importlib.util.spec_from_file_location("rca_outlook_trace_summary", MODULE_PATH)
rca = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(rca)


def empty_log_summary() -> dict:
    return {
        "stall_warnings": Counter(),
        "startup_missing_gates": Counter(),
        "query_rows_response_frames": Counter(),
        "raw_umolk_placeholder": 0,
        "unknown_getprops_tags": Counter(),
        "unknown_getprops_contexts": Counter(),
        "unknown_defaulted_getprops_tags": Counter(),
        "unknown_defaulted_getprops_contexts": Counter(),
        "problem_getprops_tags": Counter(),
        "problem_getprops_contexts": Counter(),
        "umolk_problem_getprops_tags": Counter(),
        "umolk_problem_getprops_contexts": Counter(),
        "umolk_getprops_request_ids": set(),
        "umolk_optional_defaulted_getprops_tags": Counter(),
        "umolk_optional_defaulted_getprops_contexts": Counter(),
        "associated_config_optional_defaulted_getprops_tags": Counter(),
        "associated_config_optional_defaulted_getprops_contexts": Counter(),
        "resolved_named_getprops_tags": set(),
        "zero_default_tags": Counter(),
        "descriptor_gap_windows": Counter(),
        "folder_local_default_view_visibility": Counter(),
        "folder_local_default_view_visibility_contexts": Counter(),
        "broad_ipm_configuration_row_count_gaps": Counter(),
        "visible_release_without_query_rows": 0,
        "visible_inbox_query_rows": Counter(),
        "visible_inbox_query_rows_contexts": Counter(),
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
        "umolk_dictionary_olprefs_versions": Counter(),
        "umolk_dictionary_info_versions": Counter(),
        "umolk_dictionary_issues": Counter(),
        "default_view_folder_open_without_rows": Counter(),
        "default_view_query_position_without_rows": Counter(),
        "default_view_query_position_without_rows_contexts": set(),
        "default_view_id_owners": defaultdict(set),
        "default_view_id_collision_contexts": set(),
        "default_view_id_collisions": Counter(),
        "calendar_zero_duration_timed_query_position_rows": Counter(),
        "post_calendar_query_position_named_property_probes": Counter(),
        "stale_default_view_contexts": set(),
        "stale_default_view_states": Counter(),
        "default_view_descriptor_identity_columns": Counter(),
        "hierarchy_query_windows": Counter(),
        "hierarchy_query_samples": deque(maxlen=8),
    }


class RcaOutlookTraceSummaryTests(unittest.TestCase):
    def test_unknown_getprops_counts_only_unknown_name_positions(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(tags=0x11110003,0x22220003,0x0e070003,0x33330003;"
            "names=PidTagKnown,unknown,unknown,Other;"
            "problem_tags=0x44440003:0x8004010f;"
            "zero_default_tags=0x55550003;)",
        )

        self.assertEqual(
            summary["unknown_getprops_tags"],
            Counter({"0x22220003": 1}),
        )
        self.assertEqual(
            summary["unknown_getprops_contexts"],
            Counter(
                {
                    "0x22220003;object=unknown;role=unknown;folder=unknown;"
                    "request=unknown;source=unknown-name": 1,
                }
            ),
        )
        self.assertEqual(summary["problem_getprops_tags"], Counter({"0x44440003": 1}))
        self.assertEqual(
            summary["problem_getprops_contexts"],
            Counter(
                {
                    "0x44440003;object=unknown;role=unknown;folder=unknown;"
                    "request=unknown;problem=0x44440003:0x8004010f": 1,
                }
            ),
        )
        self.assertEqual(summary["zero_default_tags"], Counter({"0x55550003": 1}))

    def test_unknown_getprops_problem_tag_is_not_counted_as_unknown_success(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=message;folder=0x0000000000050001;role=inbox;"
            "tags=0x22220003;names=unknown;problem_tags=0x22220003:0x8004010f;"
            "zero_default_tags=;response=0x00000000)",
            {
                "mapi_request_id": "{REQ}:9",
                "object_kind": "message",
                "folder_id": "0x0000000000050001",
            },
        )

        self.assertEqual(summary["unknown_getprops_tags"], Counter())
        self.assertEqual(summary["unknown_getprops_contexts"], Counter())
        self.assertEqual(summary["problem_getprops_tags"], Counter({"0x22220003": 1}))
        self.assertEqual(
            summary["problem_getprops_contexts"],
            Counter(
                {
                    "0x22220003;object=message;role=inbox;"
                    "folder=0x0000000000050001;request={REQ}:9;"
                    "problem=0x22220003:0x8004010f": 1,
                }
            ),
        )

    def test_umolk_problem_getprops_uses_associated_config_class(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=associated_config;folder=0x0000000000050001;"
            "role=inbox;tags=0x90010003;names=unknown;"
            "problem_tags=0x90010003:0x8004010f;zero_default_tags=;"
            "response=0x00000000)",
            {
                "mapi_request_id": "{REQ}:128",
                "object_kind": "associated_config",
                "associated_config_class": "IPM.Configuration.UMOLK.UserOptions",
            },
        )

        self.assertEqual(summary["problem_getprops_tags"], Counter())
        self.assertEqual(
            summary["umolk_problem_getprops_tags"], Counter({"0x90010003": 1})
        )
        self.assertEqual(
            summary["umolk_problem_getprops_contexts"],
            Counter(
                {
                    "0x90010003;object=associated_config;role=inbox;"
                    "folder=0x0000000000050001;request={REQ}:128;"
                    "class=IPM.Configuration.UMOLK.UserOptions;"
                    "problem=0x90010003:0x8004010f": 1,
                }
            ),
        )

    def test_umolk_problem_getprops_uses_materialization_request_id(self) -> None:
        summary = empty_log_summary()
        summary["umolk_getprops_request_ids"].add("{REQ}:128")

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=associated_config;folder=0x0000000000050001;"
            "role=inbox;tags=0x90010003;names=unknown;"
            "problem_tags=0x90010003:0x8004010f;zero_default_tags=;"
            "response=0x00000000)",
            {
                "mapi_request_id": "{REQ}:128",
                "object_kind": "associated_config",
            },
        )

        self.assertEqual(summary["problem_getprops_tags"], Counter())
        self.assertEqual(
            summary["umolk_problem_getprops_tags"], Counter({"0x90010003": 1})
        )

    def test_unknown_getprops_context_uses_structured_fields(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=message;folder=0x0000000000050001;"
            "role=inbox;tags=0x22220003;names=unknown;problem_tags=;"
            "zero_default_tags=;response=0x00000000)",
            {
                "mapi_request_id": "{REQ}:7",
                "object_kind": "message",
                "folder_id": "0x0000000000050001",
            },
        )

        self.assertEqual(
            summary["unknown_getprops_contexts"],
            Counter(
                {
                    "0x22220003;object=message;role=inbox;"
                    "folder=0x0000000000050001;request={REQ}:7;"
                    "source=unknown-name;response=0x00000000": 1
                }
            ),
        )

    def test_unknown_getprops_defaulted_tags_are_separated(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=associated_config;folder=0x0000000000050001;"
            "role=inbox;tags=0x22220003,0x33330003;names=unknown,unknown;"
            "problem_tags=;zero_default_tags=0x22220003;response=0x00000000)",
            {"object_kind": "associated_config"},
        )

        self.assertEqual(summary["unknown_getprops_tags"], Counter({"0x33330003": 1}))
        self.assertEqual(summary["unknown_defaulted_getprops_tags"], Counter())
        self.assertEqual(
            summary["associated_config_optional_defaulted_getprops_tags"],
            Counter({"0x22220003": 1}),
        )
        self.assertEqual(summary["unknown_defaulted_getprops_contexts"], Counter())
        self.assertEqual(
            summary["associated_config_optional_defaulted_getprops_contexts"],
            Counter(
                {
                    "0x22220003;object=associated_config;role=inbox;"
                    "folder=0x0000000000050001;request=unknown;"
                    "source=unknown-name-defaulted;response=0x00000000": 1
                }
            ),
        )

    def test_umolk_getprops_defaulted_tags_are_actionable(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=associated_config;folder=0x0000000000050001;"
            "role=inbox;tags=0x90010003;names=unknown;"
            "problem_tags=;zero_default_tags=0x90010003;response=0x00000000)",
            {
                "object_kind": "associated_config",
                "associated_config_class": "IPM.Configuration.UMOLK.UserOptions",
            },
        )

        self.assertEqual(
            summary["umolk_optional_defaulted_getprops_tags"],
            Counter({"0x90010003": 1}),
        )
        self.assertEqual(
            summary["associated_config_optional_defaulted_getprops_tags"],
            Counter(),
        )
        self.assertIn(
            "umolk_optional_defaulted_getprops_type:0x0003",
            rca.issue_buckets({"nonzero_response_codes": Counter(), "parse_errors": Counter()}, summary, None),
        )

    def test_non_config_unknown_getprops_defaulted_tags_remain_actionable(self) -> None:
        summary = empty_log_summary()

        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(kind=message;folder=0x0000000000050001;"
            "role=inbox;tags=0x22220003;names=unknown;"
            "problem_tags=;zero_default_tags=0x22220003;response=0x00000000)",
            {"object_kind": "message"},
        )

        self.assertEqual(
            summary["unknown_defaulted_getprops_tags"],
            Counter({"0x22220003": 1}),
        )
        self.assertEqual(summary["associated_config_optional_defaulted_getprops_tags"], Counter())

    def test_resolved_named_context_suppresses_unknown_getprops_tags(self) -> None:
        summary = empty_log_summary()

        rca.record_resolved_named_property_context(
            summary,
            {
                "named_property_context": (
                    "0x801f001f:id=0x801f:type=0x001f:source=session:"
                    "guid=8603020000000000c000000000000046:name=content-class"
                )
            },
        )
        rca.inspect_contract(
            summary,
            "GetPropertiesSpecific(tags=0x801f001f,0x90010003;"
            "names=unknown,unknown;)",
        )

        self.assertEqual(summary["unknown_getprops_tags"], Counter({"0x90010003": 1}))

    def test_descriptor_gap_classifies_associated_and_visible_tables(self) -> None:
        summary = empty_log_summary()

        rca.record_descriptor_gap(
            summary,
            "folder_local_default_supported=true;"
            "selected_view_name=Compact;"
            "selected_missing_descriptor_columns=0x0037001f",
            {"associated": True, "folder_role": "inbox"},
        )
        rca.record_descriptor_gap(
            summary,
            "outlook_default_view_setcolumns:folder=drafts;"
            "role=calendar;"
            "selected_view_name=Calendar;"
            "selected_missing_descriptor_columns=0x0e070003",
        )

        self.assertEqual(
            summary["descriptor_gap_windows"],
            Counter(
                {
                    "associated;role=inbox;view=Compact;missing=0x0037001f": 1,
                    "visible;role=calendar;view=Calendar;missing=0x0e070003": 1,
                }
            ),
        )

    def test_visible_inbox_descriptor_contract_flags_old_compact_shape(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_context(
            summary,
            "visible_inbox_release_without_query_rows:"
            "role=inbox;selected_view_name=Compact;"
            "descriptor_summary=version=8;column_count=13;"
            "visible_column_tags=0x67480014,0x674a0014,0x674d0014,0x674e0003,"
            "0x00170003,0x8514000b,0x001a001f,0x0e170003,0x0e1b000b,"
            "0x0042001f,0x0037001f,0x0e060040,0x12130003;"
            "descriptor_query_window=total_rows=1;position=0;requested=40;"
            "selected_missing_descriptor_columns=",
        )

        issues = summary["visible_release_descriptor_contract_issues"]
        self.assertEqual(len(issues), 1)
        issue = next(iter(issues))
        self.assertIn("missing_expected=0x8503000b", issue)
        self.assertIn("0x67480014:identity_folder_id", issue)
        self.assertIn("0x001a001f:unicode_message_class", issue)
        self.assertIn(
            "visible_inbox_descriptor_contract:",
            ",".join(
                rca.issue_buckets(
                    {"nonzero_response_codes": Counter(), "parse_errors": Counter()},
                    summary,
                    Path("LPE_last_test.log"),
                )
            ),
        )

    def test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_context(
            summary,
            "visible_inbox_release_without_query_rows:"
            "role=inbox;selected_view_name=Compact;"
            "descriptor_summary=version=8;column_count=11;"
            "visible_column_tags=0x00170003,0x8503000b,0x001a001e,0x10900003,"
            "0x0e1b000b,0x0042001e,0x0037001e,0x0e060040,0x0e080003,"
            "0x9000101e;"
            "descriptor_query_window=total_rows=1;position=0;requested=40;"
            "selected_missing_descriptor_columns=",
        )

        self.assertEqual(
            summary["visible_release_descriptor_contract_issues"],
            Counter({"role=inbox;view=Compact;contract=ms_oxocfg_ok": 1}),
        )
        self.assertNotIn(
            "visible_inbox_descriptor_contract",
            ",".join(
                rca.issue_buckets(
                    {"nonzero_response_codes": Counter(), "parse_errors": Counter()},
                    summary,
                    Path("LPE_last_test.log"),
                )
            ),
        )

    def test_default_view_descriptor_identity_columns_are_reported_by_role(self) -> None:
        summary = empty_log_summary()

        rca.record_default_view_descriptor_identity_columns(
            summary,
            "default_view_folder_open:request_id={REQ}:1;role=contacts;"
            "selected_view_name=Contacts;"
            "descriptor_summary=version=8;column_count=6;"
            "visible_column_tags=0x67480014,0x674a0014,0x3001001f",
        )

        self.assertEqual(
            summary["default_view_descriptor_identity_columns"],
            Counter(
                {
                    "role=contacts;view=Contacts;"
                    "identity=0x67480014:PidTagFolderId,0x674a0014:PidTagMid": 1
                }
            ),
        )

    def test_common_view_descriptor_getprops_contract_is_reported(self) -> None:
        summary = empty_log_summary()

        rca.record_common_view_descriptor_getprops(
            summary,
            "found=true;folder_id=0x0000000000050001;"
            "view_id=0x7fffffffffe90001;view_name=Compact;"
            "requested_descriptor_tags=0x70010102,0x7002001f;"
            "response_values=0x70010102:PidTagViewDescriptorBinary:bin(512)|"
            "0x7002001f:PidTagViewDescriptorStrings:string(80)",
            {},
        )

        self.assertEqual(
            summary["common_view_descriptor_getprops"],
            Counter(
                {
                    "found=true;view=Compact;folder=0x0000000000050001;"
                    "view_id=0x7fffffffffe90001;requested=0x70010102,0x7002001f": 1
                }
            ),
        )
        self.assertEqual(summary["common_view_descriptor_getprops_issues"], Counter())

    def test_common_view_descriptor_getprops_dedupes_surface_and_debug_events(self) -> None:
        summary = empty_log_summary()
        contract = (
            "found=true;folder_id=0x0000000000050001;"
            "view_id=0x7fffffffffe90001;view_name=Compact;"
            "requested_descriptor_tags=0x68350102,0x683c0102;"
            "descriptor_column_count=11;descriptor_strings_terminators=11;"
            "descriptor_strings_starts_with_terminator=true;"
            "descriptor_strings_ends_with_terminator=true;"
            "descriptor_strings_trailing_nul=false;"
            "response_values=0x68350102:OutlookCommonViewDescriptorBinary6835:bin(512)|"
            "0x683c0102:OutlookCommonViewDescriptorStrings683C:bin(80)"
        )

        rca.record_common_view_descriptor_getprops(summary, contract, {})
        rca.record_common_view_descriptor_getprops(summary, contract, {})

        self.assertEqual(
            summary["common_view_descriptor_getprops"],
            Counter(
                {
                    "found=true;view=Compact;folder=0x0000000000050001;"
                    "view_id=0x7fffffffffe90001;requested=0x68350102,0x683c0102": 1
                }
            ),
        )
        self.assertEqual(summary["common_view_descriptor_getprops_issues"], Counter())

    def test_common_view_descriptor_getprops_flags_malformed_inbox_compact_contract(
        self,
    ) -> None:
        summary = empty_log_summary()

        rca.record_common_view_descriptor_getprops(
            summary,
            "found=true;folder_id=0x0000000000050001;"
            "view_id=0x7fffffffffe90001;view_name=Compact;"
            "requested_descriptor_tags=0x68350102,0x683c0102;"
            "descriptor_column_count=14;descriptor_strings_terminators=10;"
            "descriptor_strings_starts_with_terminator=false;"
            "descriptor_strings_ends_with_terminator=true;"
            "descriptor_strings_trailing_nul=true;"
            "response_values=0x68350102:OutlookCommonViewDescriptorBinary6835:bin(512)|"
            "0x683c0102:OutlookCommonViewDescriptorStrings683C:bin(80)",
            {},
        )

        self.assertEqual(
            summary["common_view_descriptor_getprops_issues"],
            Counter(
                {
                    "descriptor_contract=columns=14,string_terminators=10,"
                    "strings_start=false,strings_trailing_nul=true;"
                    "found=true;view=Compact;folder=0x0000000000050001;"
                    "view_id=0x7fffffffffe90001;requested=0x68350102,0x683c0102": 1
                }
            ),
        )

    def test_missing_common_view_descriptor_getprops_is_actionable(self) -> None:
        summary = empty_log_summary()

        rca.record_common_view_descriptor_getprops(
            summary,
            "found=false;folder_id=0x0000000000050001;"
            "view_id=0x7fffffffffe90001;"
            "requested_descriptor_tags=0x70010102,0x7002001f",
            {},
        )

        self.assertEqual(len(summary["common_view_descriptor_getprops_issues"]), 1)
        self.assertIn(
            "common_view_descriptor_getprops:",
            ",".join(
                rca.issue_buckets(
                    {"nonzero_response_codes": Counter(), "parse_errors": Counter()},
                    summary,
                    Path("LPE_last_test.log"),
                )
            ),
        )

    def test_structured_common_view_descriptor_getprops_requires_requested_values(self) -> None:
        summary = empty_log_summary()

        rca.record_common_view_descriptor_getprops(
            summary,
            "",
            {
                "folder_id": "0x0000000000050001",
                "view_message_id": "0x7fffffffffe90001",
                "view_name": "Compact",
                "requested_property_tags": "0x70010102,0x7002001f",
                "requested_view_descriptor_contract": (
                    "version=false;name=false;binary=true;strings=true"
                ),
                "requested_view_descriptor_response_values": (
                    "0x70010102:PidTagViewDescriptorBinary:bin(512)"
                ),
            },
        )

        self.assertEqual(
            summary["common_view_descriptor_getprops_issues"],
            Counter(
                {
                    "missing_response_values=strings;found=unknown;view=Compact;"
                    "folder=0x0000000000050001;view_id=0x7fffffffffe90001;"
                    "requested=0x70010102,0x7002001f": 1
                }
            ),
        )

    def test_calendar_query_position_without_rows_flags_zero_duration_timed_row(self) -> None:
        summary = empty_log_summary()

        rca.record_default_view_query_position_without_rows(
            summary,
            "calendar_query_position_wire:query_rows_observed=false;"
            "next_expected_client_step=query_rows_on_calendar_contents_table;"
            "selected_row_projection=event_total=1;index=0;title=Test;"
            "duration_minutes=0;all_day=false;zero_duration_timed=true",
        )

        self.assertEqual(
            summary["calendar_zero_duration_timed_query_position_rows"],
            Counter(
                {
                    "title=Test;duration=0;next=query_rows_on_calendar_contents_table": 1
                }
            ),
        )

    def test_issue_buckets_reports_visible_descriptor_gap(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(
                {"visible;role=calendar;view=Calendar;missing=0xdead0003": 1}
            ),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn("visible_descriptor_gap", rca.issue_buckets(rr, log, None))

    def test_issue_buckets_ignores_visible_descriptor_gap_for_backed_columns(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(
                {
                    "visible;role=drafts;view=Messages;"
                    "missing=0x67480014,0x674a0014,0x674d0014,0x674e0003,"
                    "0x00710102,0x0e070003,0x30080040,0x0f030102,0x85ef000b,"
                    "0x00170003,0x8514000b,0x0e170003,0x0e1b000b,0x0042001f,"
                    "0x0037001f,0x0e060040,0x12130003,0x0e1d001f,0x0e69000b,"
                    "0x0e080003,0x0fff0102,0x0ff60102,0x65e00102,0x65e10102,"
                    "0x65e20102,0x65e30102,0x67a40014": 1
                }
            ),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertNotIn("visible_descriptor_gap", rca.issue_buckets(rr, log, None))

    def test_issue_buckets_ignores_nonactionable_zero_default_tag(self) -> None:
        log = empty_log_summary()
        log["zero_default_tags"] = Counter({"0x120c0102": 3, "0x36df0102": 4})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(rca.issue_buckets(rr, log, None), ["no_server_issue_detected"])

    def test_issue_buckets_keeps_stall_symptoms_for_zero_default_noise(self) -> None:
        log = empty_log_summary()
        log["zero_default_tags"] = Counter({"0x120c0102": 3})
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            [
                "stall:after_common_views_inbox_notification_without_contents",
                "missing_gate:normal_inbox_visible_row_observed",
            ],
        )

    def test_issue_buckets_keeps_stall_symptoms_without_concrete_issue(self) -> None:
        log = empty_log_summary()
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            [
                "stall:after_common_views_inbox_notification_without_contents",
                "missing_gate:normal_inbox_visible_row_observed",
            ],
        )

    def test_issue_buckets_reports_problem_getprops_before_stall_symptoms(self) -> None:
        log = empty_log_summary()
        log["problem_getprops_tags"] = Counter({"0x120c0102": 1})
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            ["problem_getprops:0x120c0102"],
        )

    def test_issue_buckets_reports_umolk_problem_getprops_before_stall_symptoms(self) -> None:
        log = empty_log_summary()
        log["umolk_problem_getprops_tags"] = Counter(
            {"0x8a1c0048": 1, "0x859f000b": 2}
        )
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            [
                "umolk_problem_getprops_type:0x000b",
                "umolk_problem_getprops_type:0x0048",
            ],
        )

    def test_problem_getprops_bucket_order_is_stable_for_ties(self) -> None:
        log = empty_log_summary()
        log["problem_getprops_tags"] = Counter({"0x90010003": 1, "0x120c0102": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            ["problem_getprops:0x120c0102", "problem_getprops:0x90010003"],
        )

    def test_visible_inbox_query_rows_event_is_tracked(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_inbox_query_rows(
            summary,
            {
                "mapi_request_id": "request:64",
                "query_rows_context": (
                    "handle=27;position=0;requested_row_count=40;"
                    "row_summary=total=1;start=0;forward=true;returned=1"
                ),
            },
        )

        self.assertEqual(
            summary["visible_inbox_query_rows"],
            Counter({"returned=1;position=0;requested=40": 1}),
        )
        self.assertIn(
            "request=request:64;handle=27;position=0;requested_row_count=40;"
            "row_summary=total=1;start=0;forward=true;returned=1",
            summary["visible_inbox_query_rows_contexts"],
        )

    def test_query_rows_response_frames_are_counted_by_signature(self) -> None:
        summary = empty_log_summary()

        rca.record_query_rows_response_frames(
            summary,
            {
                "mapi_request_id": "request:64",
                "input_handle_table_summary": "count=1;handles=0x0000001c",
                "response_rop_frames": (
                    "0x18@0..11:len=11:out=0:rv=0x00000000:"
                    "preview=1800000000000000000000|"
                    "0x15@11..141:len=130:out=0:rv=0x00000000:"
                    "preview=15000000000001010000010000000000"
                ),
            },
            "SeekRow>QueryRows",
        )

        self.assertEqual(
            summary["query_rows_response_frames"],
            Counter(
                {
                    "signature=SeekRow>QueryRows;"
                    "handles=count=1;handles=0x0000001c;"
                    "rows=1;origin=0x01;text=none": 1,
                    "nonempty;request=request:64;signature=SeekRow>QueryRows;"
                    "handles=count=1;handles=0x0000001c;"
                    "rows=1;origin=0x01;text=none": 1,
                }
            ),
        )

    def test_query_rows_preview_text_hint_decodes_utf16_samples(self) -> None:
        preview = (
            "150000000000010100000100000000000005010000000000013f01003f010000"
            "00000000000004000000490050004d002e0043006f006e0066006900670075"
            "0072006100740069006f006e00"
        )

        self.assertIn("IPM.Configuration", rca.query_rows_preview_text_hint(preview))

    def test_issue_buckets_suppresses_visible_inbox_missing_gate_when_rows_tracked(
        self,
    ) -> None:
        log = empty_log_summary()
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        log["visible_inbox_query_rows"] = Counter(
            {"returned=1;position=0;requested=40": 1}
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, Path("LPE_last_test.log")),
            ["no_server_issue_detected"],
        )

    def test_actionable_descriptor_gap_counts_filters_backed_columns(self) -> None:
        counts = rca.actionable_descriptor_gap_counts(
            Counter(
                {
                    "visible;role=calendar;view=Calendar;missing=0x67480014,0x0e070003": 3,
                    "visible;role=calendar;view=Calendar;missing=0xdead0003": 2,
                    "associated;role=inbox;view=Compact;missing=0xdead0003": 5,
                }
            )
        )

        self.assertEqual(
            counts,
            Counter({"visible;role=calendar;view=Calendar;missing=0xdead0003": 2}),
        )

    def test_folder_local_default_view_visibility_missing_is_actionable(self) -> None:
        summary = empty_log_summary()
        fields = {
            "folder_local_default_view_visibility": (
                "folder=0x0000000000050001;role=inbox;view=0x7fffffffffe90001;"
                "name=Compact;expected=true;present=false;associated_row_count=7"
            )
        }

        rca.record_folder_local_default_view_visibility(summary, fields)

        self.assertEqual(
            summary["folder_local_default_view_visibility"],
            Counter({"role=inbox;name=Compact;present=false": 1}),
        )
        self.assertIn(
            "folder_local_default_view_missing_from_fai",
            rca.issue_buckets(
                {"nonzero_response_codes": Counter(), "parse_errors": Counter()},
                summary,
                Path("LPE_last_test.log"),
            ),
        )

    def test_broad_ipm_configuration_row_count_gap_is_actionable(self) -> None:
        summary = empty_log_summary()
        fields = {
            "folder_role": "inbox",
            "associated": True,
            "table_restriction_decoded": (
                "content;property_tag=0x001a001f;fuzzy_low=0x0002;"
                "fuzzy_high=0x0001;value=IPM.Configuration."
            ),
            "table_total_row_count": "2",
            "ipm_configuration_contract_summary": "rows=7;not_selected_required_columns=;",
            "mapi_request_id": "{session}:60",
            "current_position": "1",
        }

        rca.record_broad_ipm_configuration_row_count_gap(summary, fields)

        self.assertEqual(
            summary["broad_ipm_configuration_row_count_gaps"],
            Counter(
                {
                    "request={session}:60;position=1;table_rows=2;config_rows=7;missing=5": 1
                }
            ),
        )
        self.assertIn(
            "broad_ipm_configuration_row_count_gap:"
            "request={session}:60;position=1;table_rows=2;config_rows=7;missing=5",
            rca.issue_buckets(
                {"nonzero_response_codes": Counter(), "parse_errors": Counter()},
                summary,
                Path("LPE_last_test.log"),
            ),
        )

    def test_unknown_getprops_tag_classes_group_unconfirmed_ranges(self) -> None:
        counts = rca.unknown_tag_class_counts(
            Counter(
                {
                    "0x90010003": 2,
                    "0x6707001f": 3,
                    "0x69040102": 5,
                    "0x10830003": 7,
                    "0x65c60003": 11,
                }
            )
        )

        self.assertEqual(
            counts,
            Counter(
                {
                    "named-or-dynamic": 2,
                    "provider-defined-internal": 3,
                    "outlook-or-store-private": 5,
                    "unconfirmed-standard-range": 7,
                    "cfxics-unspecified-int32": 11,
                }
            ),
        )

    def test_post_visible_release_followups_classify_execute_state(self) -> None:
        summary = empty_log_summary()

        rca.record_post_visible_release_followup(
            summary,
            {
                "post_visible_release_hierarchy_query_position_count": "31",
                "post_visible_inbox_release_create_save_batch_count": "2",
                "default_view_normal_query_rows_observed": True,
                "normal_inbox_contents_query_rows_observed": False,
                "outlook_umolk_named_property_probe_count": "1",
                "outlook_umolk_getprops_not_found_count": "0",
            },
        )

        self.assertEqual(summary["post_visible_release_hierarchy_query_position_max"], 31)
        self.assertEqual(
            summary["post_visible_release_followups"],
            Counter(
                {
                    "hierarchy_query_position_after_visible_release": 1,
                    "create_save_batch_after_visible_release": 1,
                    "default_view_rows_elsewhere_without_inbox_rows": 1,
                    "umolk_materialized_before_stop": 1,
                }
            ),
        )

    def test_visible_release_classifies_valid_projection_before_query_rows(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_classification(
            summary,
            "visible_inbox_release_without_query_rows=true;row_count=1;"
            "column_support=backed=0x67480014,0x0037001f;defaulted=;"
            "selected_missing_descriptor_columns=;"
            "descriptor_sort_tag=0x0e060040;table_primary_sort_tag=0x0e060040",
        )

        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"valid_projection_complete_setcolumns_before_query_rows": 1}),
        )

    def test_visible_release_classifies_incomplete_projection_before_query_rows(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_classification(
            summary,
            "visible_inbox_release_without_query_rows=true;row_count=1;"
            "column_support=backed=0x67480014;defaulted=0x0037001f;"
            "selected_missing_descriptor_columns=0x0037001f",
        )

        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"incomplete_projection_before_query_rows": 1}),
        )

    def test_visible_release_classifies_descriptor_table_mismatch_before_query_rows(
        self,
    ) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_classification(
            summary,
            "visible_inbox_release_without_query_rows=true;row_count=1;"
            "defaulted=;selected_missing_descriptor_columns=0xdead0003;"
            "descriptor_columns_missing_from_table=0x00170003,0x8514000b;"
            "descriptor_sort_tag=0x0e060040;table_primary_sort_tag=0x0e060040",
        )

        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"descriptor_table_mismatch_before_query_rows": 1}),
        )

    def test_visible_release_classifies_descriptor_superset_client_subset(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_classification(
            summary,
            "visible_inbox_release_without_query_rows=true;row_count=1;"
            "defaulted=;selected_missing_descriptor_columns=;"
            "descriptor_columns_missing_from_table=0x00170003,0x8514000b;"
            "descriptor_sort_tag=0x0e060040;table_primary_sort_tag=0x0e060040",
        )

        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"descriptor_superset_client_subset_before_query_rows": 1}),
        )

    def test_view_trace_classifies_only_direct_visible_release_event(self) -> None:
        summary = empty_log_summary()

        rca.inspect_view_trace(
            summary,
            "hierarchy_query_rows:last_visible_release=handle=33;"
            "visible_inbox_release_without_query_rows=true>"
            "visible_inbox_release_without_query_rows:row_count=1;defaulted=;"
            "selected_missing_descriptor_columns=;table_sort_matches_descriptor=true",
        )

        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"valid_projection_complete_setcolumns_before_query_rows": 1}),
        )

    def test_standalone_visible_release_context_is_classified(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_context(
            summary,
            "request_id={A}:141;handle=29;row_count=1;"
            "column_support=backed=0x67480014,0x0037001f;defaulted=;"
            "selected_missing_descriptor_columns=;"
            "descriptor_sort_tag=0x0e060040;table_primary_sort_tag=0x0e060040",
        )

        self.assertEqual(summary["visible_release_without_query_rows"], 0)
        self.assertEqual(
            summary["visible_release_classifications"],
            Counter({"valid_projection_complete_setcolumns_before_query_rows": 1}),
        )

    def test_visible_release_request_metrics_are_counted(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_context(
            summary,
            "release_request_shape=mixed_setcolumns_release_batch;"
            "release_input_index=2;release_response_index=0;"
            "release_rop_count=3;release_batch_rop_count=4;"
            "release_same_execute_already_released=false;"
            "release_handle_slots_before=0:0x00000001|1:0x00000021;"
            "release_live_handle_count_before=5;"
            "release_query_position_seen_before_release=true;"
            "release_findrow_seen_before_release=false;"
            "release_query_rows_seen_before_release=false;"
            "release_content_sync_seen_before_release=false;"
            "request_rops=SetColumns,Release,Release,Release;row_count=1;"
            "defaulted=;selected_missing_descriptor_columns=;"
            "table_sort_matches_descriptor=true",
        )

        self.assertEqual(
            summary["visible_release_request_shapes"],
            Counter(
                {
                    "shape=mixed_setcolumns_release_batch;"
                    "rops=SetColumns,Release,Release,Release;in=2;out=0;"
                    "release_rops=3;batch_rops=4;duplicate=false": 1
                }
            ),
        )
        self.assertEqual(
            summary["visible_release_pre_release_states"],
            Counter(
                {
                    "query_position=true;findrow=false;query_rows=false;"
                    "content_sync=false;live_handles=5": 1
                }
            ),
        )
        self.assertEqual(
            summary["visible_release_handle_slots"],
            Counter({"0:0x00000001|1:0x00000021": 1}),
        )

    def test_visible_release_setcolumns_shape_is_counted(self) -> None:
        summary = empty_log_summary()

        rca.record_visible_release_context(
            summary,
            "request_id={A}:141;columns=0x67480014,0x674a0014,0x674d0014,"
            "0x674e0003,0x0037001f,0x0e060040;sort=0x0e060040:1;"
            "default_view_projection_kind=identity_probe_subset;"
            "descriptor_columns_not_selected=0x00170003,0x8503000b;"
            "descriptor_columns_missing_from_table=;"
            "table_contract=ms_oxocfg_ok;row_count=1;"
            "defaulted=;selected_missing_descriptor_columns=",
        )

        self.assertEqual(
            summary["visible_release_setcolumns_shapes"],
            Counter(
                {
                    "columns=0x67480014,0x674a0014,0x674d0014,0x674e0003,"
                    "0x0037001f,0x0e060040;sort=0x0e060040:1;"
                    "projection=identity_probe_subset;"
                    "descriptor_not_selected=0x00170003,0x8503000b;"
                    "descriptor_missing_from_table=;contract=ms_oxocfg_ok": 1
                }
            ),
        )

    def test_view_trace_records_terminal_events_after_visible_release(self) -> None:
        summary = empty_log_summary()

        rca.inspect_view_trace(
            summary,
            "visible_inbox_release_without_query_rows:row_count=1;defaulted=;"
            "selected_missing_descriptor_columns=;table_sort_matches_descriptor=true>"
            "hierarchy_query_rows:request_id={A}:199;role=ipm_subtree;"
            "queried_position=9;response_row_count=6;"
            "after_visible_inbox_release_without_query_rows=true>"
            "default_view_folder_open:request_id={A}:201;role=journal;name=Journal",
        )

        self.assertEqual(
            summary["post_visible_release_terminal_events"],
            Counter(
                {
                    "event=hierarchy_query_rows;role=ipm_subtree;"
                    "queried=9;rows=6;request={A}:199": 1,
                    "event=default_view_folder_open;role=journal;"
                    "view=Journal;request={A}:201": 1,
                }
            ),
        )
        self.assertEqual(
            list(summary["post_visible_release_terminal_tail"]),
            [
                "event=hierarchy_query_rows;role=ipm_subtree;"
                "queried=9;rows=6;request={A}:199",
                "event=default_view_folder_open;role=journal;"
                "view=Journal;request={A}:201",
            ],
        )

    def test_default_view_id_collision_records_reused_folder_local_view_id(self) -> None:
        summary = empty_log_summary()

        rca.inspect_view_trace(
            summary,
            "default_view_advertised:request_id={A}:1;"
            "owner_folder=0x0000000000100001;view_folder=0x0000000000100001;"
            "view=0x7fffffffffe90001;name=Calendar>"
            "default_view_advertised:request_id={A}:2;"
            "owner_folder=0x0000000000110001;view_folder=0x0000000000110001;"
            "view=0x7fffffffffe90001;name=Journal",
        )

        self.assertEqual(
            summary["default_view_id_collisions"],
            Counter(
                {
                    "view=0x7fffffffffe90001;"
                    "owners=0x0000000000100001,0x0000000000110001": 1
                }
            ),
        )

    def test_setcolumns_release_response_frame_is_counted_from_execute_fields(self) -> None:
        summary = empty_log_summary()

        rca.record_setcolumns_release_response(
            summary,
            {
                "request_rop_names": "SetColumns,Release,Release,Release",
                "response_rop_frames": "0x12@0..7:len=7:out=33:rv=0x00000000",
                "request_rop_raw_frames": "0x12@0..30:len=30:logon=0:in=0:out=-:payload=27|0x01@30..33:len=3:logon=0:in=0:out=-:payload=0",
                "output_handle_table_summary": "count=1;handles=0x00000022",
            },
        )

        self.assertEqual(
            summary["setcolumns_release_response_frames"],
            Counter({"0x12@0..7:len=7:out=33:rv=0x00000000": 1}),
        )
        self.assertEqual(
            summary["setcolumns_release_response_handle_classifications"],
            Counter({"released_slot_reused_in_response_handle_table": 1}),
        )

    def test_setcolumns_release_response_classifies_invalidated_handle_slot(self) -> None:
        summary = empty_log_summary()

        rca.record_setcolumns_release_response(
            summary,
            {
                "request_rop_names": "SetColumns,Release,Release,Release",
                "request_rop_raw_frames": "0x12@0..30:len=30:logon=0:in=0:out=-:payload=27|0x01@30..33:len=3:logon=0:in=0:out=-:payload=0",
                "output_handle_table_summary": "count=1;handles=0x00000000",
            },
        )

        self.assertEqual(
            summary["setcolumns_release_response_handle_classifications"],
            Counter({"ms_oxcrops_release_invalidated_handle_table_entry": 1}),
        )

    def test_setcolumns_release_response_checks_all_release_slots(self) -> None:
        summary = empty_log_summary()

        rca.record_setcolumns_release_response(
            summary,
            {
                "request_rop_names": "SetColumns,Release,Release,Release",
                "request_rop_raw_frames": "0x12@0..30:len=30:logon=0:in=0:out=-:payload=27|0x01@30..33:len=3:logon=3:in=1:out=-:payload=0|0x01@33..36:len=3:logon=3:in=2:out=-:payload=0|0x01@36..39:len=3:logon=0:in=0:out=-:payload=0",
                "output_handle_table_summary": "count=1;handles=0x00000022",
            },
        )

        self.assertEqual(
            summary["setcolumns_release_response_handle_classifications"],
            Counter({"released_slot_reused_in_response_handle_table": 1}),
        )

    def test_setcolumns_release_response_classifies_generic_execute_copy_without_raw_frames(
        self,
    ) -> None:
        summary = empty_log_summary()

        rca.record_setcolumns_release_response(
            summary,
            {
                "request_rop_names": "SetColumns,Release,Release,Release",
                "output_handle_table_summary": "count=1;handles=0x00000000",
            },
        )

        self.assertEqual(
            summary["setcolumns_release_response_handle_classifications"],
            Counter({"ms_oxcrops_release_invalidated_handle_table_entry": 1}),
        )

    def test_rr_summary_counts_setcolumns_release_response_frame(self) -> None:
        event = {
            "direction": "outbound",
            "phase": "Execute",
            "endpoint": "emsmdb",
            "session_id": "session-1",
            "response_status": 200,
            "metadata": {
                "request_rop_names": "SetColumns,Release,Release,Release",
                "request_handle_table": "count=3;handles=0x00000022,0x00000056,0x00000055",
                "response_rop_frames": "0x12@0..7:len=7:out=33:rv=0x00000000",
                "response_handle_table_bytes": "4",
                "response_rop_buffer_preview": "000004000d000d0009001200000000000021",
            },
        }

        class FakePath:
            def open(self, *args, **kwargs):
                return io.StringIO(json.dumps(event) + "\n")

        original_trace_jsonl_paths = rca.trace_jsonl_paths
        try:
            rca.trace_jsonl_paths = lambda trace_dir: [FakePath()]
            summary = rca.summarize_rr(Path("unused"))
        finally:
            rca.trace_jsonl_paths = original_trace_jsonl_paths

        self.assertEqual(
            summary["setcolumns_release_response_frames"],
            Counter({"0x12@0..7:len=7:out=33:rv=0x00000000": 1}),
        )
        self.assertEqual(
            summary["setcolumns_release_response_previews"],
            Counter({"000004000d000d0009001200000000000021": 1}),
        )
        self.assertEqual(
            summary["setcolumns_release_response_handle_classifications"],
            Counter({"released_slot_non_request_handle_in_response_handle_table": 1}),
        )

    def test_rr_summary_classifies_stale_released_handle(self) -> None:
        metadata = {
            "request_handle_table": "count=3;handles=0x00000022,0x00000056,0x00000055",
            "response_handle_table_bytes": "4",
            "response_rop_buffer_preview": "000004000d000d0009001200000000000022000000",
        }

        self.assertEqual(
            rca.classify_rr_setcolumns_release_response(metadata),
            "released_slot_reused_in_response_handle_table",
        )

    def test_rr_summary_classifies_invalidated_released_handle(self) -> None:
        metadata = {
            "request_handle_table": "count=3;handles=0x00000022,0x00000056,0x00000055",
            "response_handle_table_bytes": "4",
            "response_rop_buffer_preview": "000004000d000d0009001200000000000000000000",
        }

        self.assertEqual(
            rca.classify_rr_setcolumns_release_response(metadata),
            "released_slot_invalidated_in_response_handle_table",
        )

    def test_umolk_dictionary_shapes_are_counted_from_context(self) -> None:
        summary = empty_log_summary()

        rca.record_umolk_dictionary_shapes(
            summary,
            "request_id=x;dictionary_shape=xml_user_configuration_dictionary;"
            "response_shape=values",
        )

        self.assertEqual(
            summary["umolk_dictionary_shapes"],
            Counter({"xml_user_configuration_dictionary": 1}),
        )

    def test_umolk_dictionary_olprefs_versions_are_counted_from_context(self) -> None:
        summary = empty_log_summary()

        rca.record_umolk_dictionary_shapes(
            summary,
            "request_id=x;dictionary_shape=xml_user_configuration_dictionary;"
            "dictionary_olprefs_version=positive;dictionary_olprefs_value=9-1;"
            "dictionary_info_version=Outlook.16;response_shape=values",
        )

        self.assertEqual(
            summary["umolk_dictionary_olprefs_versions"],
            Counter({"positive;value=9-1": 1}),
        )
        self.assertEqual(
            summary["umolk_dictionary_info_versions"],
            Counter({"Outlook.16": 1}),
        )

    def test_umolk_dictionary_contract_flags_requested_missing_dictionary(self) -> None:
        summary = empty_log_summary()

        rca.record_umolk_dictionary_shapes(
            summary,
            "request_id=x;config=0x1;class=IPM.Configuration.UMOLK.UserOptions;"
            "dictionary_shape=dictionary_not_returned;"
            "tags=0x001a001f,0x7c060003,0x7c070102;"
            "values=0x001a001f:string:chars=35,0x7c060003:i32",
        )

        self.assertEqual(
            summary["umolk_dictionary_issues"],
            Counter(
                {
                    "missing=0x7c070102:PidTagRoamingDictionary;"
                    "request=x;config=0x1;class=IPM.Configuration.UMOLK.UserOptions": 1
                }
            ),
        )

    def test_umolk_dictionary_contract_ignores_dictionary_not_requested(self) -> None:
        summary = empty_log_summary()

        rca.record_umolk_dictionary_shapes(
            summary,
            "request_id=x;dictionary_shape=dictionary_not_returned;"
            "tags=0x001a001f,0x0e0b0102;"
            "values=0x001a001f:string:chars=35,0x0e0b0102:binary:bytes=170",
        )

        self.assertEqual(summary["umolk_dictionary_issues"], Counter())

    def test_default_view_query_position_without_rows_classifies_calendar(self) -> None:
        summary = empty_log_summary()

        rca.record_default_view_query_position_without_rows(
            summary,
            "calendar_query_position_wire:request_id=x;"
            "query_rows_observed=false;"
            "next_expected_client_step=query_rows_on_calendar_contents_table",
        )

        self.assertEqual(
            summary["default_view_query_position_without_rows"],
            Counter(
                {
                    "role=calendar;next=query_rows_on_calendar_contents_table": 1,
                }
            ),
        )

    def test_default_view_query_position_without_rows_classifies_generic_role(self) -> None:
        summary = empty_log_summary()

        rca.record_default_view_query_position_without_rows(
            summary,
            "default_view_query_position_wire:request_id=x;"
            "query_rows_observed=false;"
            "next_expected_client_step=query_rows_on_tasks_contents_table;"
            "folder=0x0000000000130001;role=tasks",
        )

        self.assertEqual(
            summary["default_view_query_position_without_rows"],
            Counter(
                {
                    "role=tasks;next=query_rows_on_tasks_contents_table": 1,
                }
            ),
        )

    def test_query_position_wire_fields_are_classified_directly(self) -> None:
        summary = empty_log_summary()

        rca.record_query_position_wire_fields(
            summary,
            {
                "default_view_query_position_wire": (
                    "request_id=x;query_rows_observed=false;"
                    "next_expected_client_step=query_rows_on_notes_contents_table;"
                    "folder=0x0000000000120001;role=notes"
                )
            },
        )

        self.assertEqual(
            summary["default_view_query_position_without_rows"],
            Counter(
                {
                    "role=notes;next=query_rows_on_notes_contents_table": 1,
                }
            ),
        )

    def test_query_position_wire_deduplicates_direct_and_trace_event(self) -> None:
        summary = empty_log_summary()
        wire = (
            "request_id=x;query_rows_observed=false;"
            "next_expected_client_step=query_rows_on_calendar_contents_table"
        )

        rca.record_query_position_wire_fields(
            summary, {"calendar_query_position_wire": wire}
        )
        rca.record_default_view_query_position_without_rows(
            summary, f"calendar_query_position_wire:{wire}"
        )

        self.assertEqual(
            summary["default_view_query_position_without_rows"],
            Counter({"role=calendar;next=query_rows_on_calendar_contents_table": 1}),
        )

    def test_default_view_folder_open_without_rows_classifies_role_and_folder(self) -> None:
        summary = empty_log_summary()

        rca.record_default_view_folder_open_without_rows(
            summary,
            {
                "default_view_folder_open_without_query_rows": True,
                "last_default_view_folder_open_context": (
                    "request_id=x;handle=139;folder=0x0000000000100001;"
                    "role=calendar;container_class=IPF.Appointment"
                ),
            },
        )

        self.assertEqual(
            summary["default_view_folder_open_without_rows"],
            Counter({"role=calendar;folder=0x0000000000100001": 1}),
        )

    def test_post_calendar_query_position_named_property_probe_is_bucketed(self) -> None:
        summary = empty_log_summary()

        rca.record_post_calendar_query_position_named_property_probe(
            summary,
            {
                "object_kind": "folder",
                "requested_named_property_count": 3,
                "pre_resolution_missing_named_property_count": 1,
                "unresolved_returned_property_id_count": 0,
                "returned_property_id_sources": "well_known=2,newly_allocated=1",
                "calendar_query_position_context": (
                    "handle=139;response_position=0;response_row_count=1"
                ),
            },
        )

        self.assertEqual(
            summary["post_calendar_query_position_named_property_probes"],
            Counter(
                {
                    "object=folder;requested=3;missing=1;unresolved=0;"
                    "sources=well_known=2,newly_allocated=1;"
                    "calendar_position=0;calendar_rows=1": 1,
                }
            ),
        )

    def test_rr_event_tail_summary_keeps_endpoint_phase_and_codes(self) -> None:
        summary = rca.rr_event_tail_summary(
            {
                "direction": "outbound",
                "endpoint": "emsmdb",
                "phase": "Execute",
                "response_status": 200,
                "response_body_bytes": 42,
            },
            {
                "mapi_request_id": "{REQ}:7",
                "request_rop_names": "SetColumns,QueryPosition",
                "mapi_response_code": "0",
            },
        )

        self.assertEqual(
            summary,
            "outbound:emsmdb:Execute;request={REQ}:7;"
            "rops=SetColumns,QueryPosition;http=200;mapi=0;bytes=42",
        )

    def test_trace_dir_for_log_uses_matching_child_run_directory(self) -> None:
        root = MODULE_PATH.parent / ".rca_trace_dir_for_log_test"
        shutil.rmtree(root, ignore_errors=True)
        try:
            root.mkdir()
            run = root / "202607070648"
            run.mkdir()
            (run / "outlook-mapi-session.rr.jsonl").write_text("{}", encoding="utf-8")
            log = root / "LPE_last_202607070648.log"

            self.assertEqual(rca.trace_dir_for_log(root, log), run)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_trace_dir_for_log_uses_nearest_child_run_directory(self) -> None:
        root = MODULE_PATH.parent / ".rca_trace_dir_for_log_test"
        shutil.rmtree(root, ignore_errors=True)
        try:
            root.mkdir()
            older = root / "202607070646"
            older.mkdir()
            (older / "outlook-mapi-old.rr.jsonl").write_text("{}", encoding="utf-8")
            nearest = root / "202607070649"
            nearest.mkdir()
            (nearest / "outlook-mapi-new.rr.jsonl").write_text("{}", encoding="utf-8")
            log = root / "LPE_last_202607070648.log"

            self.assertEqual(rca.trace_dir_for_log(root, log), nearest)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_issue_buckets_ignores_context_only_post_visible_release_followup(self) -> None:
        log = empty_log_summary()
        log["post_visible_release_followups"] = Counter(
            {
                "hierarchy_query_position_after_visible_release": 3,
                "default_view_rows_elsewhere_without_inbox_rows": 2,
                "umolk_materialized_before_stop": 1,
            }
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(rca.issue_buckets(rr, log, None), ["no_server_issue_detected"])

    def test_issue_buckets_reports_actionable_post_visible_release_followup(self) -> None:
        log = empty_log_summary()
        log["post_visible_release_followups"] = Counter(
            {"create_save_batch_after_visible_release": 1}
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "post_visible_release:create_save_batch_after_visible_release",
            rca.issue_buckets(rr, log, None),
        )

    def test_issue_buckets_reports_default_view_query_position_without_rows(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(
                {"role=calendar;next=query_rows_on_calendar_contents_table": 1}
            ),
            "post_calendar_query_position_named_property_probes": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "default_view_query_position_without_rows:"
            "role=calendar;next=query_rows_on_calendar_contents_table",
            rca.issue_buckets(rr, log, None),
        )

    def test_issue_buckets_reports_default_view_folder_open_without_rows(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(
                {"role=calendar;folder=0x0000000000100001": 1}
            ),
            "default_view_query_position_without_rows": Counter(),
            "post_calendar_query_position_named_property_probes": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "default_view_folder_open_without_rows:"
            "role=calendar;folder=0x0000000000100001",
            rca.issue_buckets(rr, log, None),
        )

    def test_issue_buckets_reports_default_view_id_collision(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "visible_release_classifications": Counter(),
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(),
            "default_view_id_collisions": Counter(
                {
                    "view=0x7fffffffffe90001;"
                    "owners=0x0000000000100001,0x0000000000110001": 1
                }
            ),
            "post_calendar_query_position_named_property_probes": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "default_view_id_collision:view=0x7fffffffffe90001;"
            "owners=0x0000000000100001,0x0000000000110001",
            rca.issue_buckets(rr, log, None),
        )

    def test_issue_buckets_ignores_complete_projection_visible_release_classification(self) -> None:
        log = empty_log_summary()
        log["visible_release_without_query_rows"] = 1
        log["visible_release_classifications"] = Counter(
            {"valid_projection_complete_setcolumns_before_query_rows": 1}
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.issue_buckets(rr, log, None),
            ["no_server_issue_detected"],
        )

    def test_issue_buckets_reports_unclassified_visible_release(self) -> None:
        log = empty_log_summary()
        log["visible_release_without_query_rows"] = 1
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn("visible_inbox_release_before_query_rows", rca.issue_buckets(rr, log, None))

    def test_issue_buckets_reports_actionable_visible_release_classification(self) -> None:
        log = empty_log_summary()
        log["visible_release_without_query_rows"] = 1
        log["visible_release_classifications"] = Counter(
            {"incomplete_projection_before_query_rows": 1}
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "visible_inbox_release_classification:incomplete_projection_before_query_rows",
            rca.issue_buckets(rr, log, None),
        )

    def test_verdict_treats_descriptor_superset_visible_release_as_actionable(self) -> None:
        log = empty_log_summary()
        log["visible_release_without_query_rows"] = 1
        log["visible_release_classifications"] = Counter(
            {"descriptor_superset_client_subset_before_query_rows": 1}
        )
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.verdict_for_summary(rr, log, Path("LPE_last_test.log")),
            "transport is clean; journal diagnostics contain actionable MAPI/view issues.",
        )

    def test_verdict_prioritizes_concrete_issue_over_stall_symptoms(self) -> None:
        log = empty_log_summary()
        log["visible_release_without_query_rows"] = 1
        log["visible_release_classifications"] = Counter(
            {"incomplete_projection_before_query_rows": 1}
        )
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.verdict_for_summary(rr, log, Path("LPE_last_test.log")),
            "transport is clean; journal diagnostics contain actionable MAPI/view issues.",
        )

    def test_verdict_keeps_stall_message_when_only_stall_symptoms_exist(self) -> None:
        log = empty_log_summary()
        log["stall_warnings"] = Counter(
            {"after_common_views_inbox_notification_without_contents": 1}
        )
        log["startup_missing_gates"] = Counter({"normal_inbox_visible_row_observed": 1})
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.verdict_for_summary(rr, log, Path("LPE_last_test.log")),
            "transport is clean; startup stall diagnostics identify a server-side MAPI bootstrap stop.",
        )

    def test_issue_buckets_reports_post_calendar_named_property_probe(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(),
            "post_calendar_query_position_named_property_probes": Counter(
                {"object=folder;requested=3;missing=1;unresolved=0;sources=x;calendar_position=0;calendar_rows=1": 1}
            ),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "post_calendar_query_position_named_property_probe:"
            "object=folder;requested=3;missing=1;unresolved=0;"
            "sources=x;calendar_position=0;calendar_rows=1",
            rca.issue_buckets(rr, log, None),
        )

    def test_verdict_treats_post_calendar_named_property_probe_as_actionable(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(),
            "post_calendar_query_position_named_property_probes": Counter(
                {"object=folder;requested=3;missing=1;unresolved=0;sources=x;calendar_position=0;calendar_rows=1": 1}
            ),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertEqual(
            rca.verdict_for_summary(rr, log, Path("LPE_last_test.log")),
            "transport is clean; journal diagnostics contain actionable MAPI/view issues.",
        )

    def test_build_scope_identifies_current_clean_and_dirty_builds(self) -> None:
        self.assertEqual(
            rca.build_scope_for("fb8dd0e77a76", "clean", "fb8dd0e7"),
            "current-clean-build",
        )
        self.assertEqual(
            rca.build_scope_for("fb8dd0e77a76", "dirty", "fb8dd0e7"),
            "current-dirty-build",
        )
        self.assertEqual(
            rca.build_scope_for("602251ee1dfe", "clean", "fb8dd0e7"),
            "old-build",
        )

    def test_print_build_issue_counts_accepts_custom_title(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            rca.print_build_issue_counts(
                Counter({("fb8dd0e77a76/clean", "visible_descriptor_gap"): 1}),
                "Current-build issue buckets",
            )

        self.assertIn("Current-build issue buckets", output.getvalue())
        self.assertIn("fb8dd0e77a76/clean,visible_descriptor_gap: 1", output.getvalue())

    def test_issue_buckets_reports_setcolumns_release_handle_classifications(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "visible_release_classifications": Counter(),
            "setcolumns_release_response_handle_classifications": Counter(
                {"released_slot_reused_in_response_handle_table": 1}
            ),
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(),
            "default_view_id_collisions": Counter(),
            "calendar_zero_duration_timed_query_position_rows": Counter(),
            "post_calendar_query_position_named_property_probes": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {
            "nonzero_response_codes": Counter(),
            "parse_errors": Counter(),
            "setcolumns_release_response_handle_classifications": Counter(
                {"released_slot_reused_in_response_handle_table": 1}
            ),
        }

        self.assertIn(
            "setcolumns_release_response_handle:"
            "released_slot_reused_in_response_handle_table",
            rca.issue_buckets(rr, log, None),
        )
        self.assertIn(
            "rr_setcolumns_release_response_handle:"
            "released_slot_reused_in_response_handle_table",
            rca.issue_buckets(rr, log, None),
        )

    def test_issue_buckets_ignores_expected_setcolumns_release_handle_classifications(
        self,
    ) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "visible_release_classifications": Counter(),
            "setcolumns_release_response_handle_classifications": Counter(
                {
                    "ms_oxcrops_release_invalidated_handle_table_entry": 1,
                    "ms_oxcrops_release_trimmed_unreferenced_handle_table_entry": 1,
                }
            ),
            "post_visible_release_followups": Counter(),
            "default_view_folder_open_without_rows": Counter(),
            "default_view_query_position_without_rows": Counter(),
            "default_view_id_collisions": Counter(),
            "calendar_zero_duration_timed_query_position_rows": Counter(),
            "post_calendar_query_position_named_property_probes": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {
            "nonzero_response_codes": Counter(),
            "parse_errors": Counter(),
            "setcolumns_release_response_handle_classifications": Counter(
                {"released_slot_invalidated_in_response_handle_table": 1}
            ),
        }

        self.assertEqual(rca.issue_buckets(rr, log, None), ["no_server_issue_detected"])

    def test_batch_summary_prints_current_setcolumns_release_handle_classifications(
        self,
    ) -> None:
        with self.subTest("current build aggregation"):
            trace_root = Path(self._testMethodName)
            trace_dir = trace_root / "202607071712"
            trace_dir.mkdir(parents=True, exist_ok=True)

            log = empty_log_summary()
            log.update(
                {
                    "build": {
                        "git_commit": "aae96d21ed4b",
                        "git_dirty": "",
                    },
                    "sequence_counts": Counter(),
                    "startup_missing_gates": Counter(),
                    "stall_warnings": Counter(),
                    "raw_umolk_placeholder": 0,
                }
            )
            log["setcolumns_release_response_handle_classifications"] = Counter(
                {"released_slot_reused_in_response_handle_table": 1}
            )
            rr = {
                "events": 1,
                "nonzero_response_codes": Counter(),
                "parse_errors": Counter(),
                "setcolumns_release_response_frames": Counter(),
                "setcolumns_release_response_handle_classifications": Counter(
                    {"released_slot_reused_in_response_handle_table": 1}
                ),
            }
            log_path = Path("LPE_last_202607071712.log")

            original_indexed_log_files = rca.indexed_log_files
            original_matching_log_for_run = rca.matching_log_for_run
            original_summarize_rr = rca.summarize_rr
            original_summarize_log = rca.summarize_log
            try:
                rca.indexed_log_files = lambda _logs_root: {}
                rca.matching_log_for_run = lambda _run, _logs: log_path
                rca.summarize_rr = lambda _trace_dir: rr
                rca.summarize_log = lambda _log_path: log

                output = io.StringIO()
                with redirect_stdout(output):
                    rca.print_batch_summary(trace_root, Path("."), "aae96d21")
            finally:
                rca.indexed_log_files = original_indexed_log_files
                rca.matching_log_for_run = original_matching_log_for_run
                rca.summarize_rr = original_summarize_rr
                rca.summarize_log = original_summarize_log
                shutil.rmtree(trace_root)

            text = output.getvalue()
            self.assertIn("Current-build runs matched: 1", text)
            self.assertIn(
                "Current-build Journal SetColumns+Release response handle classifications",
                text,
            )
            self.assertIn("released_slot_reused_in_response_handle_table: 1", text)
            self.assertIn(
                "aae96d21ed4b/clean,setcolumns_release_response_handle:"
                "released_slot_reused_in_response_handle_table: 1",
                text,
            )

    def test_batch_summary_warns_when_current_build_has_no_matching_runs(self) -> None:
        trace_root = Path(self._testMethodName)
        trace_dir = trace_root / "202607071736"
        trace_dir.mkdir(parents=True, exist_ok=True)

        log = empty_log_summary()
        log.update(
            {
                "build": {
                    "git_commit": "aae96d21ed4b",
                    "git_dirty": "",
                },
                "sequence_counts": Counter(),
                "startup_missing_gates": Counter(),
                "stall_warnings": Counter(),
                "raw_umolk_placeholder": 0,
            }
        )
        rr = {
            "events": 1,
            "nonzero_response_codes": Counter(),
            "parse_errors": Counter(),
            "setcolumns_release_response_frames": Counter(),
            "setcolumns_release_response_handle_classifications": Counter(),
        }
        log_path = Path("LPE_last_202607071736.log")

        original_indexed_log_files = rca.indexed_log_files
        original_matching_log_for_run = rca.matching_log_for_run
        original_summarize_rr = rca.summarize_rr
        original_summarize_log = rca.summarize_log
        try:
            rca.indexed_log_files = lambda _logs_root: {}
            rca.matching_log_for_run = lambda _run, _logs: log_path
            rca.summarize_rr = lambda _trace_dir: rr
            rca.summarize_log = lambda _log_path: log

            output = io.StringIO()
            with redirect_stdout(output):
                rca.print_batch_summary(trace_root, Path("."), "af25fd18")
        finally:
            rca.indexed_log_files = original_indexed_log_files
            rca.matching_log_for_run = original_matching_log_for_run
            rca.summarize_rr = original_summarize_rr
            rca.summarize_log = original_summarize_log
            shutil.rmtree(trace_root)

        text = output.getvalue()
        self.assertIn("Current-build runs matched: 0", text)
        self.assertIn(
            "Current-build warning: no Outlook trace/log pair matched the "
            "requested build prefix af25fd18",
            text,
        )


if __name__ == "__main__":
    unittest.main()
