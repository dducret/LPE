---
type: Rust Method
title: navigation_shortcut_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1333-L1340
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table
---

# Signature

`pub(crate) fn navigation_shortcut_message_for_id( &self, item_id: u64, ) -> Option<MapiNavigationShortcutMessage>`

# Calls

- [navigation_shortcut_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages.md)

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [current_common_views_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [navigation_shortcut_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)