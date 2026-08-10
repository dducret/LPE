---
type: Rust Function
title: mapi_post_handler
resource: crates/lpe-exchange/src/service.rs#L260-L295
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_emsmdb_post_handler
  - functions/crates/lpe-exchange/src/service/mapi_nspi_post_handler
---

# Signature

`async fn mapi_post_handler( endpoint: MapiEndpoint, storage: Storage, uri: Uri, headers: HeaderMap, body: Bytes, ) -> Response`

# Calls

- [mapi_error_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response.md)
- [log_mapi_transport_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)

# Called by

- [mapi_emsmdb_post_handler](../../../../../functions/crates/lpe-exchange/src/service/mapi_emsmdb_post_handler.md)
- [mapi_nspi_post_handler](../../../../../functions/crates/lpe-exchange/src/service/mapi_nspi_post_handler.md)