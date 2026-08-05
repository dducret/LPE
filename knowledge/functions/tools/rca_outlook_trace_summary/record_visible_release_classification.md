---
type: Python Function
title: record_visible_release_classification
resource: tools/rca_outlook_trace_summary.py#L1303-L1354
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/int_text_field
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_valid_projection_before_query_rows
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_incomplete_projection_before_query_rows
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_descriptor_table_mismatch_before_query_rows
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_descriptor_superset_client_subset
---

# Signature

`def record_visible_release_classification(summary: dict[str, Any], text: str) -> None:`

# Calls

- [int_text_field](../../../functions/tools/rca_outlook_trace_summary/int_text_field.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)
- [test_visible_release_classifies_valid_projection_before_query_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_valid_projection_before_query_rows.md)
- [test_visible_release_classifies_incomplete_projection_before_query_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_incomplete_projection_before_query_rows.md)
- [test_visible_release_classifies_descriptor_table_mismatch_before_query_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_descriptor_table_mismatch_before_query_rows.md)
- [test_visible_release_classifies_descriptor_superset_client_subset](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_classifies_descriptor_superset_client_subset.md)