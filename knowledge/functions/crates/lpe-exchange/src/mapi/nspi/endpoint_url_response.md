---
type: Rust Function
title: endpoint_url_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L165-L194
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/public_endpoint_url
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn endpoint_url_response( request_type: &str, request_id: &str, headers: &HeaderMap, path: &str, ) -> Response`

# Calls

- [public_endpoint_url](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/public_endpoint_url.md)
- [write_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)