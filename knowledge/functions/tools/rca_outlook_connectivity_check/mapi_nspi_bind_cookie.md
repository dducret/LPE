---
type: Python Function
title: mapi_nspi_bind_cookie
resource: tools/rca_outlook_connectivity_check.py#L807-L834
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/http/require_guid_counter_header
  - functions/tools/rca_outlook/http/cookie_header
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_bind_octet_stream
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
---

# Signature

`def mapi_nspi_bind_cookie(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> str:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [require_guid_counter_header](../../../functions/tools/rca_outlook/http/require_guid_counter_header.md)
- [cookie_header](../../../functions/tools/rca_outlook/http/cookie_header.md)

# Called by

- [check_mapi_nspi_bind_octet_stream](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_bind_octet_stream.md)
- [check_mapi_nspi_address_book](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)