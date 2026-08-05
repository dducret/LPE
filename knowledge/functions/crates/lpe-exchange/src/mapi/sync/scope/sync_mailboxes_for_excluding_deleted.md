---
type: Rust Function
title: sync_mailboxes_for_excluding_deleted
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L83-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/mailbox_is_hierarchy_descendant
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder
  - functions/crates/lpe-exchange/src/mapi/sync/scope/hierarchy_virtual_folder_ids
  - functions/crates/lpe-exchange/src/mapi/sync/scope/special_folder_is_in_sync_scope
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/tests/deleted_advertised_quick_step_folder_is_excluded_from_hierarchy_sync
---

# Signature

`pub(in crate::mapi) fn sync_mailboxes_for_excluding_deleted( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], deleted_advertised_special_folders: &HashSet<u64>, ) -> Vec<JmapMailbox>`

# Calls

- [mailbox_is_hierarchy_descendant](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/mailbox_is_hierarchy_descendant.md)
- [mailbox_shadowed_by_active_outlook_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder.md)
- [hierarchy_virtual_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/hierarchy_virtual_folder_ids.md)
- [special_folder_is_in_sync_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/special_folder_is_in_sync_scope.md)
- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [sync_mailboxes_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for.md)
- [sync_state_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for_excluding_deleted.md)
- [deleted_advertised_quick_step_folder_is_excluded_from_hierarchy_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/deleted_advertised_quick_step_folder_is_excluded_from_hierarchy_sync.md)