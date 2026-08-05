---
type: Python Function
title: record_post_visible_release_followup
resource: tools/rca_outlook_trace_summary.py#L971-L1013
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/int_field
  - functions/tools/rca_outlook_trace_summary/is_truthy
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_post_visible_release_followups_classify_execute_state
---

# Signature

`def record_post_visible_release_followup( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [int_field](../../../functions/tools/rca_outlook_trace_summary/int_field.md)
- [is_truthy](../../../functions/tools/rca_outlook_trace_summary/is_truthy.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_post_visible_release_followups_classify_execute_state](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_post_visible_release_followups_classify_execute_state.md)