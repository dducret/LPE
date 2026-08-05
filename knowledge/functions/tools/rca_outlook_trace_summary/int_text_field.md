---
type: Python Function
title: int_text_field
resource: tools/rca_outlook_trace_summary.py#L1046-L1050
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap
  - functions/tools/rca_outlook_trace_summary/record_visible_release_classification
  - functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def int_text_field(text: str, key: str) -> int:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_broad_ipm_configuration_row_count_gap](../../../functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap.md)
- [record_visible_release_classification](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_classification.md)
- [record_default_view_query_position_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows.md)
- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)