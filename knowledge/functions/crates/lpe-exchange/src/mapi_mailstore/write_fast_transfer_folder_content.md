---
type: Rust Function
title: write_fast_transfer_folder_content
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L880-L921
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_emails_for_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments
---

# Signature

`fn write_fast_transfer_folder_content( buffer: &mut Vec<u8>, folder_id: u64, mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], top_folder: bool, )`

# Calls

- [write_fast_transfer_folder_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [fast_transfer_emails_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_emails_for_folder.md)
- [fast_transfer_message_list_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments.md)
- [fast_transfer_child_mailboxes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes.md)
- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [fast_transfer_top_folder_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments.md)