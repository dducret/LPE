---
type: Rust Function
title: fast_transfer_manifest_buffer_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L714-L774
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_visible_recipient_facts
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`pub(crate) fn fast_transfer_manifest_buffer_with_attachments( folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], ) -> Vec<u8>`

# Calls

- [canonical_folder_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number.md)
- [write_prefixed_bytes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_prefixed_bytes.md)
- [source_key_for_mailbox_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)
- [canonical_message_change_number_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)
- [source_key_for_uuid](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [canonical_message_flags](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags.md)
- [canonical_flag_status](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status.md)
- [write_visible_recipient_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_visible_recipient_facts.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)