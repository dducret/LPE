---
type: Rust Function
title: open_stream_data
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L64-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/attachment_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
---

# Signature

`pub(in crate::mapi) async fn open_stream_data<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, input_handle: u32, property_tag: u32, open_mode: u8, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Option<(Vec<u8>, Option<StreamWriteTarget>)>`

# Calls

- [attachment_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/attachment_stream_data.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)

# Called by

- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)