---
type: Python Function
title: record_common_views_fai_transfer_summary
resource: tools/rca_outlook_trace_summary.py#L766-L784
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/int_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_string8_subject_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_subject_is_not_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_missing_message_flag_is_actionable
---

# Signature

`def record_common_views_fai_transfer_summary( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [int_field](../../../functions/tools/rca_outlook_trace_summary/int_field.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_common_views_fai_unicode_string8_subject_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_string8_subject_is_actionable.md)
- [test_common_views_fai_unicode_subject_is_not_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_unicode_subject_is_not_actionable.md)
- [test_common_views_fai_missing_message_flag_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_views_fai_missing_message_flag_is_actionable.md)