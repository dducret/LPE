---
type: Rust Function
title: sync_mailboxes_with_collaboration_counts
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1051-L1114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/collaboration_folder_in_hierarchy_sync_scope
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(super) fn sync_mailboxes_with_collaboration_counts( mut mailboxes: Vec<JmapMailbox>, snapshot: &MapiMailStoreSnapshot, sync_root_folder_id: u64, sync_type: u8, ) -> Vec<JmapMailbox>`

# Calls

- [try_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [folder_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [events_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)
- [collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [collaboration_folder_in_hierarchy_sync_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/collaboration_folder_in_hierarchy_sync_scope.md)
- [collaboration_folder_identity_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)