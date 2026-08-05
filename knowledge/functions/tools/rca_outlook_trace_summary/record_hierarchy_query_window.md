---
type: Python Function
title: record_hierarchy_query_window
resource: tools/rca_outlook_trace_summary.py#L1066-L1083
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/first_hierarchy_row
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
---

# Signature

`def record_hierarchy_query_window( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [first_hierarchy_row](../../../functions/tools/rca_outlook_trace_summary/first_hierarchy_row.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)