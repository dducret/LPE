---
type: Python Function
title: record_default_view_id_collision
resource: tools/rca_outlook_trace_summary.py#L1256-L1273
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
---

# Signature

`def record_default_view_id_collision(summary: dict[str, Any], segment: str) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)

# Called by

- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)