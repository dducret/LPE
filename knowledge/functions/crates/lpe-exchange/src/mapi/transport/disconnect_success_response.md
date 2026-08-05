---
type: Rust Function
title: disconnect_success_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L573-L589
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
---

# Signature

`fn disconnect_success_response( endpoint: MapiEndpoint, request_id: &str, response_request_type: &str, ) -> Response`

# Calls

- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)

# Called by

- [disconnect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)