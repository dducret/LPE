---
type: Rust Function
title: write_stream
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L559-L585
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/properties/streams/copy_stream
---

# Signature

`pub(in crate::mapi) fn write_stream( session: &mut MapiSession, stream_handle: u32, bytes: &[u8], ) -> Option<usize>`

# Calls

- [sync_stream_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)

# Called by

- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [copy_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/copy_stream.md)