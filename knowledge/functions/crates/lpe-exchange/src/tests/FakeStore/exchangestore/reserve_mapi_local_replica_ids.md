---
type: Rust Method
title: reserve_mapi_local_replica_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L4865-L4891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_scalar_calendar_entry_id_alias_survives_a_new_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards
---

# Signature

`fn reserve_mapi_local_replica_ids<'a>( &'a self, account_id: Uuid, id_count: u32, ) -> StoreFuture<'a, u64>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_get_local_replica_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response.md)
- [mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_contact_sync_import_save_reports_deleted_source_key](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key.md)
- [mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect.md)
- [mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session.md)
- [mapi_over_http_scalar_calendar_entry_id_alias_survives_a_new_session](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_scalar_calendar_entry_id_alias_survives_a_new_session.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges.md)
- [mapi_over_http_sync_import_associated_message_persists_and_replays_fai](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink.md)
- [mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox.md)
- [mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable.md)
- [mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent.md)
- [mapi_navigation_shortcut_import_commits_content_and_identity_atomically](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically.md)
- [mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted.md)
- [postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards.md)