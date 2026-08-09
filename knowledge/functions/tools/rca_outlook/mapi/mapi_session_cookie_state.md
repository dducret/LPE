---
type: Python Function
title: mapi_session_cookie_state
resource: tools/rca_outlook/mapi.py#L94-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`def mapi_session_cookie_state(cookie: str) -> str:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [execute](../../../../functions/tools/rca_outlook_connectivity_check/execute.md)