---
type: Python Function
title: classify_mixed_release_queryposition_response
resource: tools/rca_outlook_trace_summary.py#L1428-L1462
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/parse_handle_table_summary
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_mixed_release_queryposition_response
---

# Signature

`def classify_mixed_release_queryposition_response( fields: dict[str, Any], handle_table: str ) -> str:`

# Calls

- [parse_handle_table_summary](../../../functions/tools/rca_outlook_trace_summary/parse_handle_table_summary.md)

# Called by

- [record_mixed_release_queryposition_response](../../../functions/tools/rca_outlook_trace_summary/record_mixed_release_queryposition_response.md)