---
type: Python Function
title: read_string_cell
resource: tools/rca_outlook_connectivity_check.py#L152-L165
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/mapi_gate1_hierarchy_rows
---

# Signature

`def read_string_cell() -> str:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [mapi_gate1_hierarchy_rows](../../../functions/tools/rca_outlook_connectivity_check/mapi_gate1_hierarchy_rows.md)