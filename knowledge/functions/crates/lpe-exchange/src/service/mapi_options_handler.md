---
type: Rust Function
title: mapi_options_handler
resource: crates/lpe-exchange/src/service.rs#L228-L238
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_options_handler_reports_transport_session_ready
---

# Signature

`pub(crate) async fn mapi_options_handler() -> Response`

# Calls

- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)

# Called by

- [mapi_options_handler_reports_transport_session_ready](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_options_handler_reports_transport_session_ready.md)