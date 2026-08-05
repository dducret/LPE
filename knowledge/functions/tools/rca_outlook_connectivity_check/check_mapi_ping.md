---
type: Python Function
title: check_mapi_ping
resource: tools/rca_outlook_connectivity_check.py#L735-L804
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/http/cookie_header
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_mapi_ping(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [cookie_header](../../../functions/tools/rca_outlook/http/cookie_header.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)