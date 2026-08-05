---
type: Python Function
title: classify_setcolumns_release_response_handle_table
resource: tools/rca_outlook_trace_summary.py#L1373-L1404
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/parse_handle_table_summary
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_setcolumns_release_response
---

# Signature

`def classify_setcolumns_release_response_handle_table( fields: dict[str, Any], handle_table: str ) -> str:`

# Calls

- [parse_handle_table_summary](../../../functions/tools/rca_outlook_trace_summary/parse_handle_table_summary.md)

# Called by

- [record_setcolumns_release_response](../../../functions/tools/rca_outlook_trace_summary/record_setcolumns_release_response.md)