---
type: Python Function
title: check_mapi_nspi_resolve_authenticated_mailbox
resource: tools/rca_outlook_connectivity_check.py#L1178-L1210
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/mapi/mapi_http_binary_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_mapi_nspi_resolve_authenticated_mailbox( base_url: str, email: str, password: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [mapi_nspi_bind_cookie](../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [assert_nspi_resolve_names_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)