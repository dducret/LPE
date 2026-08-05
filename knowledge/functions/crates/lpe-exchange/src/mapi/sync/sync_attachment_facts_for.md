---
type: Rust Function
title: sync_attachment_facts_for
resource: crates/lpe-exchange/src/mapi/sync.rs#L1038-L1097
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`pub(in crate::mapi) fn sync_attachment_facts_for( folder_id: u64, emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts>`

# Calls

- [mapi_folder_id_for_email](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email.md)
- [events_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)

# Called by

- [sync_attachment_facts_for_with_embedded_content](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content.md)
- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_fast_transfer_source_copy_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)