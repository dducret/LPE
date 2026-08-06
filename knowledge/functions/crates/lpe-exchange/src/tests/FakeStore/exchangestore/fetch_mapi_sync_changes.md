---
type: Rust Method
title: fetch_mapi_sync_changes
resource: crates/lpe-exchange/src/tests/mod.rs#L7220-L7229
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/tests/hierarchy_tombstones/postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
---

# Signature

`fn fetch_mapi_sync_changes<'a>( &'a self, _account_id: Uuid, _mailbox_id: Option<Uuid>, _checkpoint_kind: MapiCheckpointKind, _after_change_sequence: u64, ) -> StoreFuture<'a, MapiSyncChangeSet>`

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone](../../../../../../../functions/crates/lpe-exchange/src/tests/hierarchy_tombstones/postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone.md)
- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_associated_config_delete_tombstones_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)