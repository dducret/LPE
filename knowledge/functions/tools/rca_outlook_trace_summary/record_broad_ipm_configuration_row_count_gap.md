---
type: Python Function
title: record_broad_ipm_configuration_row_count_gap
resource: tools/rca_outlook_trace_summary.py#L720-L748
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/is_truthy
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/int_field
  - functions/tools/rca_outlook_trace_summary/int_text_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_requires_startup_row
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_ignores_suppressed_non_startup_rows
---

# Signature

`def record_broad_ipm_configuration_row_count_gap( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [is_truthy](../../../functions/tools/rca_outlook_trace_summary/is_truthy.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [int_field](../../../functions/tools/rca_outlook_trace_summary/int_field.md)
- [int_text_field](../../../functions/tools/rca_outlook_trace_summary/int_text_field.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_broad_ipm_configuration_row_count_gap_requires_startup_row](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_requires_startup_row.md)
- [test_broad_ipm_configuration_row_count_gap_ignores_suppressed_non_startup_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_ignores_suppressed_non_startup_rows.md)