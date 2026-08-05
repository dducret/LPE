---
type: Rust Function
title: filetime_from_change_number
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L344-L346
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response
  - functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for
  - functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
  - functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update
---

# Signature

`pub(crate) fn filetime_from_change_number(change_number: u64) -> u64`

# Called by

- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [public_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [common_view_named_view_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)
- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [contact_entry_id_is_private_message_entry_id_not_a_sync_key](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key.md)
- [rop_get_receive_folder_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response.md)
- [normal_message_sync_facts_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [contact_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)
- [navigation_shortcut_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [search_folder_definition_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)
- [common_view_named_view_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [conversation_action_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)
- [delegate_freebusy_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object.md)
- [associated_config_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [special_folder_property_value_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max.md)
- [email_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
- [distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)
- [mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts.md)
- [mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [commit_mapi_folder_hierarchy_change](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [fetch_or_allocate_mapi_identities](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact.md)
- [commit_mapi_navigation_shortcut_update](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update.md)