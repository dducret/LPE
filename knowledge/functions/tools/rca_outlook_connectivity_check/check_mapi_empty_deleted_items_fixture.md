---
type: Python Function
title: check_mapi_empty_deleted_items_fixture
resource: tools/rca_outlook_connectivity_check.py#L1343-L1430
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/ews/require_ews_no_error
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/cookie_header
  - functions/tools/rca_outlook/mapi/mapi_execute_body
  - functions/tools/rca_outlook/mapi/mapi_rop_buffer
  - functions/tools/rca_outlook/mapi/mapi_empty_deleted_items_rops
  - functions/tools/rca_outlook/mapi/mapi_execute_response_rops
  - functions/tools/rca_outlook/mapi/mapi_http_binary_payload
  - functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_mapi_empty_deleted_items_fixture( base_url: str, email: str, password: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [ews_call](../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require_ews_no_error](../../../functions/tools/rca_outlook/ews/require_ews_no_error.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [cookie_header](../../../functions/tools/rca_outlook/http/cookie_header.md)
- [mapi_execute_body](../../../functions/tools/rca_outlook/mapi/mapi_execute_body.md)
- [mapi_rop_buffer](../../../functions/tools/rca_outlook/mapi/mapi_rop_buffer.md)
- [mapi_empty_deleted_items_rops](../../../functions/tools/rca_outlook/mapi/mapi_empty_deleted_items_rops.md)
- [mapi_execute_response_rops](../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [check_jmap_email_subject_absent](../../../functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)