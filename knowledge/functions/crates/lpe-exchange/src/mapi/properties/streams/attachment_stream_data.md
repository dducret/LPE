---
type: Rust Function
title: attachment_stream_data
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L13-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data
---

# Signature

`pub(in crate::mapi) async fn attachment_stream_data<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, input_handle: u32, open_mode: u8, snapshot: &MapiMailStoreSnapshot, ) -> Option<(Vec<u8>, Option<StreamWriteTarget>)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [attachment_for_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)
- [fetch_attachment_content](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)

# Called by

- [open_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data.md)