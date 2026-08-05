---
type: Python Function
title: record_inbox_default_view_advertisement
resource: tools/rca_outlook_trace_summary.py#L1276-L1300
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
---

# Signature

`def record_inbox_default_view_advertisement( summary: dict[str, Any], segment: str ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)