---
type: Rust Function
title: execute_transport_failure_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L1159-L1166
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi) fn execute_transport_failure_response( request_id: &str, response_code: u16, message: &str, cookies: Vec<String>, ) -> Response`

# Calls

- [mapi_diagnostic_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)