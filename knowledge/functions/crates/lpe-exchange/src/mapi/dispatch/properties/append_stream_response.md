---
type: Rust Function
title: append_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L3-L82
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response
---

# Signature

`pub(super) async fn append_stream_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_seek_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response.md)
- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [append_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)
- [append_get_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response.md)
- [append_clone_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response.md)
- [append_stream_region_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response.md)

# Called by

- [append_stream_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response.md)