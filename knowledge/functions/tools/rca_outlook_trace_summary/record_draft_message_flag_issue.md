---
type: Python Function
title: record_draft_message_flag_issue
resource: tools/rca_outlook_trace_summary.py#L1744-L1764
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_draft_query_row_without_mf_unsent_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_draft_query_row_with_mf_unsent_is_not_actionable
---

# Signature

`def record_draft_message_flag_issue( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_draft_query_row_without_mf_unsent_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_draft_query_row_without_mf_unsent_is_actionable.md)
- [test_draft_query_row_with_mf_unsent_is_not_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_draft_query_row_with_mf_unsent_is_not_actionable.md)