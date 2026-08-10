---
type: Python Function
title: require_guid_counter_header
resource: tools/rca_outlook/http.py#L118-L122
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
---

# Signature

`def require_guid_counter_header(value: str, label: str) -> None:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [mapi_nspi_bind_cookie](../../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)