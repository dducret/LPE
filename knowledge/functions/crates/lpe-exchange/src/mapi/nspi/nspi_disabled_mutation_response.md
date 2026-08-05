---
type: Rust Function
title: nspi_disabled_mutation_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L196-L202
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn nspi_disabled_mutation_response( request_type: &str, request_id: &str, message: &str, ) -> Response`

# Calls

- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)