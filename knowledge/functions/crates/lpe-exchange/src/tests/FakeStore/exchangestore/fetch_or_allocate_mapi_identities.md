---
type: Rust Method
title: fetch_or_allocate_mapi_identities
resource: crates/lpe-exchange/src/tests/mod.rs#L5991-L6161
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically
  - functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards
  - functions/crates/lpe-exchange/src/tests/mapi_identity_allocator_rejects_an_exhausted_global_counter
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create
---

# Signature

`fn fetch_or_allocate_mapi_identities<'a>( &'a self, _account_id: Uuid, requests: &'a [MapiIdentityRequest], ) -> StoreFuture<'a, Vec<MapiIdentityRecord>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [legacy_migration_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_get_per_user_long_term_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response.md)
- [mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)
- [remember_created_mapi_identity_record](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)
- [completed_message_move_replay_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity.md)
- [remember_nspi_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records.md)
- [load_mapi_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq.md)
- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
- [mapi_identity_repair_preserves_rotated_calendar_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key.md)
- [mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql.md)
- [mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql.md)
- [mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts.md)
- [mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)
- [mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_associated_config_delete_tombstones_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
- [mapi_navigation_shortcut_import_commits_content_and_identity_atomically](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically.md)
- [mapi_identity_source_key_lookup_and_checkpoints_round_trip](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip.md)
- [postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards.md)
- [mapi_identity_allocator_rejects_an_exhausted_global_counter](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_allocator_rejects_an_exhausted_global_counter.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)
- [mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)
- [commit_mapi_navigation_shortcut_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create.md)
- [commit_mapi_associated_config_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create.md)