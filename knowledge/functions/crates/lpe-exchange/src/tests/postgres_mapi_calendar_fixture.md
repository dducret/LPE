---
type: Rust Function
title: postgres_mapi_calendar_fixture
resource: crates/lpe-exchange/src/tests/mod.rs#L201-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-exchange/src/tests/hierarchy_tombstones/postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_contents_table_projects_postgresql_canonical_event_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_id_reservations_are_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture_drop_cleans_temporary_schema
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows
  - functions/crates/lpe-exchange/src/tests/mapi_identity_allocator_rejects_an_exhausted_global_counter
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
---

# Signature

`async fn postgres_mapi_calendar_fixture() -> anyhow::Result<Option<PostgresMapiFixture>>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/hierarchy_tombstones/postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone.md)
- [mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid.md)
- [mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties.md)
- [mapi_over_http_calendar_contents_table_projects_postgresql_canonical_event_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_contents_table_projects_postgresql_canonical_event_properties.md)
- [mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event.md)
- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
- [mapi_identity_repair_preserves_rotated_calendar_change_key](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key.md)
- [mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql.md)
- [mapi_local_replica_id_reservations_are_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_id_reservations_are_atomic_in_postgresql.md)
- [mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql.md)
- [mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts.md)
- [mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql.md)
- [mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql.md)
- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)
- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)
- [mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant.md)
- [mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state.md)
- [mapi_over_http_online_associated_config_create_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_associated_config_delete_tombstones_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink.md)
- [mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)
- [postgres_mapi_calendar_fixture_drop_cleans_temporary_schema](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture_drop_cleans_temporary_schema.md)
- [postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations.md)
- [postgres_mapi_contacts_local_commit_time_tracks_canonical_update](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update.md)
- [mapi_associated_config_storage_is_account_scoped](../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped.md)
- [mapi_associated_config_upsert_preserves_canonical_message_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity.md)
- [mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects](../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects.md)
- [mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable](../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
- [mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target](../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target.md)
- [mapi_navigation_shortcut_import_commits_content_and_identity_atomically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically.md)
- [mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted.md)
- [postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards.md)
- [postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows.md)
- [mapi_identity_allocator_rejects_an_exhausted_global_counter](../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_allocator_rejects_an_exhausted_global_counter.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)
- [mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed](../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)