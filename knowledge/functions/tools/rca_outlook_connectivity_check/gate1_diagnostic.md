---
type: Python Function
title: gate1_diagnostic
resource: tools/rca_outlook_connectivity_check.py#L100-L101
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`def gate1_diagnostic(event: str, **fields: object) -> None:`

# Called by

- [check_mapi_gate1_readiness](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [execute](../../../functions/tools/rca_outlook_connectivity_check/execute.md)