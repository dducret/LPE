---
type: Python Function
title: mapi_gate1_hierarchy_rows
resource: tools/rca_outlook_connectivity_check.py#L133-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook_connectivity_check/read_string_cell
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
---

# Signature

`def mapi_gate1_hierarchy_rows(response_rops: bytes, label: str) -> list[tuple[str, str]]:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)
- [read_string_cell](../../../functions/tools/rca_outlook_connectivity_check/read_string_cell.md)

# Called by

- [check_mapi_gate1_readiness](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)