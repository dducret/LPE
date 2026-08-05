---
type: Python Function
title: record_folder_local_default_view_visibility
resource: tools/rca_outlook_trace_summary.py#L751-L763
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/field_in_semicolon_text
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_folder_local_default_view_visibility_missing_is_actionable
---

# Signature

`def record_folder_local_default_view_visibility( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [field_in_semicolon_text](../../../functions/tools/rca_outlook_trace_summary/field_in_semicolon_text.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_folder_local_default_view_visibility_missing_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_folder_local_default_view_visibility_missing_is_actionable.md)