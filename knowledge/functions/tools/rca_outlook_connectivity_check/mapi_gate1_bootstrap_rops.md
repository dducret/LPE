---
type: Python Function
title: mapi_gate1_bootstrap_rops
resource: tools/rca_outlook_connectivity_check.py#L104-L123
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/mapi_wire_folder_id
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
---

# Signature

`def mapi_gate1_bootstrap_rops(folder_counter: int, hierarchy: bool) -> bytes:`

# Calls

- [mapi_wire_folder_id](../../../functions/tools/rca_outlook/mapi/mapi_wire_folder_id.md)

# Called by

- [check_mapi_gate1_readiness](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)