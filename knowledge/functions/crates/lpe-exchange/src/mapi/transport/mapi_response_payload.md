---
type: Rust Function
title: mapi_response_payload
resource: crates/lpe-exchange/src/mapi/transport.rs#L793-L798
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
---

# Signature

`pub(in crate::mapi) fn mapi_response_payload(response: &Response) -> Option<&[u8]>`

# Called by

- [trace_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)