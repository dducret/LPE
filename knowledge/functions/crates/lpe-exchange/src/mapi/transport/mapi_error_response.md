---
type: Rust Function
title: mapi_error_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L372-L384
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_post_handler
---

# Signature

`pub(crate) fn mapi_error_response(error: &anyhow::Error) -> Response`

# Calls

- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)

# Called by

- [mapi_post_handler](../../../../../../functions/crates/lpe-exchange/src/service/mapi_post_handler.md)