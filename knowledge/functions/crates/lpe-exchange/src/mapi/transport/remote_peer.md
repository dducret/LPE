---
type: Rust Function
title: remote_peer
resource: crates/lpe-exchange/src/mapi/transport.rs#L1137-L1142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
---

# Signature

`fn remote_peer(headers: &HeaderMap) -> Option<String>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [trace_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)