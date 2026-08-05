---
type: Python Function
title: actionable_issue_buckets
resource: tools/rca_outlook_trace_summary.py#L3975-L3983
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  called_by:
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_string8_subject_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_subject_is_not_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_missing_message_flag_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_associated_findrow_rowset_violation_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_normal_view_default_entry_id_not_found_is_expected
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_logon_associated_sharing_provider_not_found_is_expected
---

# Signature

`def actionable_issue_buckets( rr: dict[str, Any], log: dict[str, Any], log_path: Path | None ) -> list[str]:`

# Calls

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)

# Called by

- [test_common_views_fai_unicode_string8_subject_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_string8_subject_is_actionable.md)
- [test_common_views_fai_unicode_subject_is_not_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_subject_is_not_actionable.md)
- [test_common_views_fai_missing_message_flag_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_missing_message_flag_is_actionable.md)
- [test_associated_findrow_rowset_violation_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_associated_findrow_rowset_violation_is_actionable.md)
- [test_calendar_normal_view_default_entry_id_not_found_is_expected](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_normal_view_default_entry_id_not_found_is_expected.md)
- [test_logon_associated_sharing_provider_not_found_is_expected](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_logon_associated_sharing_provider_not_found_is_expected.md)