---
type: Python Function
title: verdict_for_summary
resource: tools/rca_outlook_trace_summary.py#L2707-L2727
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/mismatched_capture_sessions
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_rejects_disjoint_rr_and_journal_mapi_sessions
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_treats_descriptor_superset_visible_release_as_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_prioritizes_concrete_issue_over_stall_symptoms
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_keeps_stall_message_when_only_stall_symptoms_exist
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_treats_post_calendar_named_property_probe_as_actionable
---

# Signature

`def verdict_for_summary( rr: dict[str, Any], log: dict[str, Any], log_path: Path | None ) -> str:`

# Calls

- [mismatched_capture_sessions](../../../functions/tools/rca_outlook_trace_summary/mismatched_capture_sessions.md)
- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_verdict_rejects_disjoint_rr_and_journal_mapi_sessions](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_rejects_disjoint_rr_and_journal_mapi_sessions.md)
- [test_verdict_treats_descriptor_superset_visible_release_as_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_treats_descriptor_superset_visible_release_as_actionable.md)
- [test_verdict_prioritizes_concrete_issue_over_stall_symptoms](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_prioritizes_concrete_issue_over_stall_symptoms.md)
- [test_verdict_keeps_stall_message_when_only_stall_symptoms_exist](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_keeps_stall_message_when_only_stall_symptoms_exist.md)
- [test_verdict_treats_post_calendar_named_property_probe_as_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_verdict_treats_post_calendar_named_property_probe_as_actionable.md)