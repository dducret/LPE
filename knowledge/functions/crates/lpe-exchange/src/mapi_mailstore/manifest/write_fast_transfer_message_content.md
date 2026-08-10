---
type: Rust Function
title: write_fast_transfer_message_content
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L101-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i64
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_address
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_content_buffer_with_attachments
---

# Signature

`pub(super) fn write_fast_transfer_message_content( buffer: &mut Vec<u8>, email: &JmapEmail, attachments: &[AttachmentSyncFact], durable_identity: Option<&crate::store::MapiIdentityRecord>, property_filter: FastTransferDirectPropertyFilter<'_>, message_children: FastTransferMessageChildren, )`

# Calls

- [includes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [write_i64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i64.md)
- [email_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [write_utf16_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [fast_transfer_sender_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_name.md)
- [fast_transfer_sender_address](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_address.md)
- [fast_transfer_sent_representing_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sent_representing_name.md)
- [message_class_for_email](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email.md)
- [write_fast_transfer_message_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children.md)

# Called by

- [fast_transfer_message_list_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments.md)
- [fast_transfer_message_content_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_content_buffer_with_attachments.md)