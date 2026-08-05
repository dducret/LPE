---
type: Rust Function
title: copy_stream
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L642-L662
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
---

# Signature

`pub(in crate::mapi) fn copy_stream( session: &mut MapiSession, source_handle: u32, destination_handle: u32, byte_count: u64, ) -> Option<(usize, usize)>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [write_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream.md)

# Called by

- [append_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)