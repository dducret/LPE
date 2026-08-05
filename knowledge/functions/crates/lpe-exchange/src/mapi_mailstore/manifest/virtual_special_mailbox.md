---
type: Rust Function
title: virtual_special_mailbox
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L306-L320
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/sync_upload/sync_checkpoint_scope
  - functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion
  - functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_special_object_kind_for_checkpoint_mailbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
  - functions/crates/lpe-exchange/src/tests/mapi_default_calendar_folder_identity_is_persisted
  - functions/crates/lpe-exchange/src/tests/mapi_full_snapshot_persists_virtual_special_folder_version_identity
---

# Signature

`pub(crate) fn virtual_special_mailbox(folder_id: u64) -> Option<JmapMailbox>`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [virtual_special_mailbox_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [log_calendar_identity_chain](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain.md)
- [default_folder_hierarchy_projection_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug.md)
- [sync_checkpoint_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/sync_upload/sync_checkpoint_scope.md)
- [sync_checkpoint_mailbox_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)
- [root_hierarchy_transfer_ipm_subtree_reports_virtual_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children.md)
- [ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters.md)
- [default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders.md)
- [hierarchy_transfer_calendar_includes_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id.md)
- [hierarchy_transfer_inbox_includes_calendar_identification_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id.md)
- [hierarchy_transfer_respects_entry_id_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_string8_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion.md)
- [is_virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox.md)
- [mapi_special_object_kind_for_checkpoint_mailbox](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_special_object_kind_for_checkpoint_mailbox.md)
- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_advertised_calendar_sync_projects_default_collection_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event.md)
- [mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows.md)
- [mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration.md)
- [mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy.md)
- [mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_contacts_sync_exports_associated_config_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes.md)
- [mapi_over_http_contact_content_sync_exports_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes.md)
- [mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_contacts_search_content_sync_uses_search_folder_parent](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent.md)
- [mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai.md)
- [mapi_over_http_conversation_action_content_sync_exports_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
- [mapi_default_calendar_folder_identity_is_persisted](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_default_calendar_folder_identity_is_persisted.md)
- [mapi_full_snapshot_persists_virtual_special_folder_version_identity](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_full_snapshot_persists_virtual_special_folder_version_identity.md)