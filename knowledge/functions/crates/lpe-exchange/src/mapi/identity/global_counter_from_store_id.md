---
type: Rust Function
title: global_counter_from_store_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L601-L607
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/transient_embedded_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_id_matches_or_is_persistable_alias_candidate
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/source_key_for_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/long_term_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_with_provider
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/message_entry_id_from_object_ids
  - functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_long_term_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider_and_type
  - functions/crates/lpe-exchange/src/mapi/identity/raw_message_entry_id_from_object_ids
  - functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_minimal_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/tests/long_term_id_from_id_accepts_outlook_and_emitted_counter_forms
  - functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_includes_long_term_id_source_in_trailing_replid_form
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key
  - functions/crates/lpe-exchange/src/mapi/tables/rules/rule_sequence
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
  - functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_definition_to_folder
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/preflight_unknown_mapi_navigation_shortcut_deletes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/tombstone_unknown_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_jmap_email_from_mailbox_with_mapi_identity
  - functions/crates/lpe-exchange/src/tests/strict_decode_change_number_property
---

# Signature

`pub(crate) fn global_counter_from_store_id(store_id: u64) -> Option<u64>`

# Called by

- [transient_associated_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id.md)
- [transient_client_local_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id.md)
- [transient_embedded_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/transient_embedded_message_id.md)
- [default_folder_id_matches_or_is_persistable_alias_candidate](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_id_matches_or_is_persistable_alias_candidate.md)
- [optimized_send_target](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_target.md)
- [append_submit_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [imported_fai_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)
- [from_special_folder_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/source_key_for_object_id.md)
- [long_term_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/long_term_id_from_object_id.md)
- [folder_entry_id_with_provider](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_with_provider.md)
- [message_entry_id_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/message_entry_id_from_object_ids.md)
- [raw_wire_id_bytes_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id.md)
- [raw_long_term_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_long_term_id_from_object_id.md)
- [folder_entry_id_with_provider_and_type](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider_and_type.md)
- [raw_message_entry_id_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_message_entry_id_from_object_ids.md)
- [raw_source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id.md)
- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [nspi_minimal_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_minimal_id_from_object_id.md)
- [long_term_id_from_id_accepts_outlook_and_emitted_counter_forms](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/long_term_id_from_id_accepts_outlook_and_emitted_counter_forms.md)
- [unresolved_mapi_object_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [access_plan_includes_long_term_id_source_in_trailing_replid_form](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_includes_long_term_id_source_in_trailing_replid_form.md)
- [pending_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key.md)
- [rule_sequence](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/rule_sequence.md)
- [replguid_idset_from_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [default_folder_hierarchy_membership_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary.md)
- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)
- [mapi_search_folder_definition_to_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_definition_to_folder.md)
- [outlook_default_folder_named_view_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)
- [mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_identity_repair_preserves_rotated_calendar_change_key](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key.md)
- [mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql.md)
- [mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_store_identity_is_shared_and_allocations_do_not_overlap_across_accounts.md)
- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [commit_mapi_folder_hierarchy_change](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [fetch_or_allocate_mapi_identities](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [upsert_mapi_special_folder_aliases](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact.md)
- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items.md)
- [commit_mapi_navigation_shortcut_import](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [preflight_unknown_mapi_navigation_shortcut_deletes](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/preflight_unknown_mapi_navigation_shortcut_deletes.md)
- [tombstone_unknown_mapi_navigation_shortcut](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/tombstone_unknown_mapi_navigation_shortcut.md)
- [commit_mapi_associated_config_import](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import.md)
- [move_jmap_email_from_mailbox_with_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_jmap_email_from_mailbox_with_mapi_identity.md)
- [strict_decode_change_number_property](../../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_change_number_property.md)