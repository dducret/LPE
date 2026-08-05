---
type: Rust Function
title: sync_state_token_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L412-L432
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
---

# Signature

`pub(crate) fn sync_state_token_with_attachments( sync_type: u8, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], folder_versions: &[crate::mapi_store::MapiFolderVersion], ) -> Vec<u8>`

# Calls

- [final_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [sync_state_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids.md)
- [sync_state_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)

# Called by

- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)