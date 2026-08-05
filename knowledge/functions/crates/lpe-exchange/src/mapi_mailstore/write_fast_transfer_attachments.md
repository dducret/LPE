---
type: Rust Function
title: write_fast_transfer_attachments
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1119-L1159
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/attachment_sync_fact_is_embedded_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_embedded_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children
---

# Signature

`fn write_fast_transfer_attachments(buffer: &mut Vec<u8>, attachments: &[AttachmentSyncFact])`

# Calls

- [attachment_sync_fact_is_embedded_message](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/attachment_sync_fact_is_embedded_message.md)
- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_binary_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [write_bool_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)
- [write_utf16_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [write_fast_transfer_embedded_message](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_embedded_message.md)

# Called by

- [write_fast_transfer_message_children](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children.md)