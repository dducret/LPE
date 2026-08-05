---
type: Rust Function
title: mapi_response_with_cookies
resource: crates/lpe-exchange/src/mapi/transport.rs#L684-L761
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-exchange/src/mapi/transport/insert_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/nspi/bind_response
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie
---

# Signature

`pub(in crate::mapi) fn mapi_response_with_cookies( request_type: &str, request_id: &str, response_code: u16, body: Vec<u8>, cookies: Vec<String>, ) -> Response`

# Calls

- [mapi_http_date](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date.md)
- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [insert_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/insert_header.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [bind_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [connect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [disconnect_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response.md)
- [mapi_diagnostic_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)
- [notification_wait_empty_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)