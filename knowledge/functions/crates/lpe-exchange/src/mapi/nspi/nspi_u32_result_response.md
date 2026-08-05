---
type: Rust Function
title: nspi_u32_result_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L362-L373
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn nspi_u32_result_response( request_type: &str, request_id: &str, value: u32, ) -> Response`

# Calls

- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)