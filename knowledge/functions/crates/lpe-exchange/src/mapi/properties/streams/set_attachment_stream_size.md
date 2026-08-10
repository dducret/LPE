---
type: Rust Function
title: set_attachment_stream_size
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L841-L866
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
---

# Signature

`pub(in crate::mapi) fn set_attachment_stream_size( session: &mut MapiSession, stream_handle: u32, stream_size: u64, ) -> Option<()>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [sync_stream_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)

# Called by

- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)