---
type: Rust Function
title: mapi_emsmdb_post_handler
resource: crates/lpe-exchange/src/service.rs#L242-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/mapi_post_handler
---

# Signature

`async fn mapi_emsmdb_post_handler( State(storage): State<Storage>, uri: Uri, headers: HeaderMap, body: Bytes, ) -> Response`

# Calls

- [mapi_post_handler](../../../../../functions/crates/lpe-exchange/src/service/mapi_post_handler.md)