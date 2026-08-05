---
type: Rust Function
title: nspi_update_stat_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1029-L1037
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn nspi_update_stat_response(request_id: &str) -> Response`

# Calls

- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)