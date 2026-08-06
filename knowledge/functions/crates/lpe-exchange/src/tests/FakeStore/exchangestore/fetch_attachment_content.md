---
type: Rust Method
title: fetch_attachment_content
resource: crates/lpe-exchange/src/tests/mod.rs#L11201-L11213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/streams/attachment_stream_data
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/get_attachment
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
---

# Signature

`fn fetch_attachment_content<'a>( &'a self, _account_id: Uuid, file_reference: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_submit_attachments_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email.md)
- [sync_attachment_facts_for_with_embedded_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content.md)
- [embedded_message_properties_from_attachment_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata.md)
- [attachment_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/attachment_stream_data.md)
- [get_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/get_attachment.md)
- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)