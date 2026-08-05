---
type: Python Function
title: parse_rr_response_handle_table
resource: tools/rca_outlook_trace_summary.py#L326-L342
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response
---

# Signature

`def parse_rr_response_handle_table(metadata: dict[str, Any]) -> list[str]:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [classify_rr_setcolumns_release_response](../../../functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response.md)