---
type: Python Function
title: check_mapi_nspi_bind_octet_stream
resource: tools/rca_outlook_connectivity_check.py#L837-L839
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_mapi_nspi_bind_octet_stream(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:`

# Calls

- [mapi_nspi_bind_cookie](../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)