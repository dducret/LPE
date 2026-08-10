---
type: Python Function
title: check_mapi_emsmdb_sent_message
resource: tools/rca_outlook_connectivity_check.py#L1213-L1273
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/http/cookie_header
  - functions/tools/rca_outlook/mapi/mapi_sent_subject_table_rops
  - functions/tools/rca_outlook/mapi/mapi_execute_body
  - functions/tools/rca_outlook/mapi/mapi_rop_buffer
  - functions/tools/rca_outlook/mapi/mapi_http_binary_payload
  - functions/tools/rca_outlook/mapi/mapi_execute_response_rops
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
---

# Signature

`def check_mapi_emsmdb_sent_message( base_url: str, email: str, password: str, expected_subject: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [cookie_header](../../../functions/tools/rca_outlook/http/cookie_header.md)
- [mapi_sent_subject_table_rops](../../../functions/tools/rca_outlook/mapi/mapi_sent_subject_table_rops.md)
- [mapi_execute_body](../../../functions/tools/rca_outlook/mapi/mapi_execute_body.md)
- [mapi_rop_buffer](../../../functions/tools/rca_outlook/mapi/mapi_rop_buffer.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [mapi_execute_response_rops](../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)

# Called by

- [check_ews_send_sent](../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)