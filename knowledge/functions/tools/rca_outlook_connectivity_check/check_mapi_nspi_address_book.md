---
type: Python Function
title: check_mapi_nspi_address_book
resource: tools/rca_outlook_connectivity_check.py#L1091-L1175
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
  - functions/tools/rca_outlook/mapi/assert_nspi_fixture_payload
  - functions/tools/rca_outlook/mapi/nspi_first_minimal_id
  - functions/tools/rca_outlook/http/update_cookie_header
  - functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_mapi_nspi_address_book( base_url: str, email: str, password: str, insecure_tls: bool, timeout: int, expected_name: str | None = None, expected_email: str | None = None, ) -> None:`

# Calls

- [mapi_nspi_bind_cookie](../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [assert_nspi_fixture_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_fixture_payload.md)
- [nspi_first_minimal_id](../../../functions/tools/rca_outlook/mapi/nspi_first_minimal_id.md)
- [update_cookie_header](../../../functions/tools/rca_outlook/http/update_cookie_header.md)
- [assert_nspi_get_props_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload.md)

# Called by

- [check_ews_contact_calendar_and_mapi_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)
- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)