---
type: Python Function
title: record_unknown_getprops_tag
resource: tools/rca_outlook_trace_summary.py#L2091-L2138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_contract
---

# Signature

`def record_unknown_getprops_tag( summary: dict[str, Any], tag: str, contract: str, fields: dict[str, Any] | None, source: str, ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [inspect_contract](../../../functions/tools/rca_outlook_trace_summary/inspect_contract.md)