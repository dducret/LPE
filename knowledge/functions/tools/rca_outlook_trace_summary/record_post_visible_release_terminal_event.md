---
type: Python Function
title: record_post_visible_release_terminal_event
resource: tools/rca_outlook_trace_summary.py#L1207-L1253
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
---

# Signature

`def record_post_visible_release_terminal_event( summary: dict[str, Any], segment: str ) -> None:`

# Calls

- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)