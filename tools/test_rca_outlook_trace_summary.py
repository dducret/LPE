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
        "unknown_getprops_tags": Counter(),
        "unknown_getprops_contexts": Counter(),
        "unknown_defaulted_getprops_tags": Counter(),
        "unknown_defaulted_getprops_contexts": Counter(),
        "associated_config_optional_defaulted_getprops_tags": Counter(),
        "associated_config_optional_defaulted_getprops_contexts": Counter(),
        "resolved_named_getprops_tags": set(),
        "zero_default_tags": Counter(),
        "descriptor_gap_windows": Counter(),
        "visible_release_without_query_rows": 0,
        "visible_release_contexts": set(),
        "visible_release_classifications": Counter(),
        "setcolumns_release_response_frames": Counter(),
        "visible_release_descriptor_windows": Counter(),
        "post_visible_release_followups": Counter(),
        "post_visible_release_terminal_events": Counter(),
        "post_visible_release_terminal_tail": deque(maxlen=12),
        "post_visible_release_terminal_contexts": set(),
        "post_visible_release_hierarchy_query_position_max": 0,
        "umolk_dictionary_shapes": Counter(),
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
            Counter({"0x22220003": 1, "0x44440003": 1}),
        )
        self.assertEqual(
            summary["unknown_getprops_contexts"],
            Counter(
                {
                    "0x22220003;object=unknown;role=unknown;folder=unknown;"
                    "request=unknown;source=unknown-name": 1,
                    "0x44440003;object=unknown;role=unknown;folder=unknown;"
                    "request=unknown;source=problem-tag": 1,
                }
            ),
        )
        self.assertEqual(summary["zero_default_tags"], Counter({"0x55550003": 1}))

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
            },
        )

        self.assertEqual(
            summary["setcolumns_release_response_frames"],
            Counter({"0x12@0..7:len=7:out=33:rv=0x00000000": 1}),
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
                "response_rop_frames": "0x12@0..7:len=7:out=33:rv=0x00000000",
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

    def test_issue_buckets_reports_post_visible_release_followup(self) -> None:
        log = {
            "visible_release_without_query_rows": 0,
            "post_visible_release_followups": Counter(
                {"hierarchy_query_position_after_visible_release": 3}
            ),
            "default_view_query_position_without_rows": Counter(),
            "raw_umolk_placeholder": 0,
            "stale_default_view_states": Counter(),
            "descriptor_gap_windows": Counter(),
            "stall_warnings": Counter(),
            "startup_missing_gates": Counter(),
        }
        rr = {"nonzero_response_codes": Counter(), "parse_errors": Counter()}

        self.assertIn(
            "post_visible_release:hierarchy_query_position_after_visible_release",
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


if __name__ == "__main__":
    unittest.main()
