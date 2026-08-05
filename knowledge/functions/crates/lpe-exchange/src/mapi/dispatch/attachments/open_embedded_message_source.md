---
type: Rust Function
title: open_embedded_message_source
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1262-L1331
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/default_embedded_message_properties
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_metadata_is_embedded_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
---

# Signature

`pub(super) async fn open_embedded_message_source<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, snapshot: &MapiMailStoreSnapshot, handle: u32, open_mode: u8, ) -> Option<(u64, u64, u32, HashMap<u32, MapiValue>)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [default_embedded_message_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/default_embedded_message_properties.md)
- [attachment_for_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)
- [attachment_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message.md)
- [attachment_metadata_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_metadata_is_embedded_message.md)
- [embedded_message_properties_from_attachment_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata.md)

# Called by

- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)