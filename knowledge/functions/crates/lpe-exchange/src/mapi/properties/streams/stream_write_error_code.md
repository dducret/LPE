---
type: Rust Function
title: stream_write_error_code
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L635-L640
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_write_error_code(error: StreamWriteError) -> u32`

# Called by

- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)