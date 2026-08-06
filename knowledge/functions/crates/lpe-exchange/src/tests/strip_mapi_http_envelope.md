---
type: Rust Function
title: strip_mapi_http_envelope
resource: crates/lpe-exchange/src/tests/mod.rs#L12617-L12634
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_creates_emsmdb_session
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`fn strip_mapi_http_envelope(bytes: Vec<u8>) -> Vec<u8>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [mapi_over_http_connect_creates_emsmdb_session](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_creates_emsmdb_session.md)
- [response_bytes](../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)