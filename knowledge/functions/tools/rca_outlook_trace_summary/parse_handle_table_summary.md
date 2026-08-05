---
type: Python Function
title: parse_handle_table_summary
resource: tools/rca_outlook_trace_summary.py#L1465-L1469
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response
  - functions/tools/rca_outlook_trace_summary/classify_setcolumns_release_response_handle_table
  - functions/tools/rca_outlook_trace_summary/classify_mixed_release_queryposition_response
---

# Signature

`def parse_handle_table_summary(summary: str) -> list[str]:`

# Called by

- [classify_rr_setcolumns_release_response](../../../functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response.md)
- [classify_setcolumns_release_response_handle_table](../../../functions/tools/rca_outlook_trace_summary/classify_setcolumns_release_response_handle_table.md)
- [classify_mixed_release_queryposition_response](../../../functions/tools/rca_outlook_trace_summary/classify_mixed_release_queryposition_response.md)