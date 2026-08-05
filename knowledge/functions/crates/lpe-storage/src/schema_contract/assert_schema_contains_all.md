---
type: Rust Function
title: assert_schema_contains_all
resource: crates/lpe-storage/src/schema_contract.rs#L75-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/collaboration_objects_have_canonical_projection_fields
  - functions/crates/lpe-storage/src/schema_contract/public_folder_schema_uses_canonical_tables_permissions_and_replay
  - functions/crates/lpe-storage/src/schema_contract/ews_compatibility_gap_models_are_canonical_sql_state
  - functions/crates/lpe-storage/src/schema_contract/calendar_event_attachments_use_canonical_event_and_blob_tables
  - functions/crates/lpe-storage/src/schema_contract/notes_journal_and_reminders_stay_canonical
  - functions/crates/lpe-storage/src/schema_contract/collaboration_rights_are_canonical_and_same_tenant
  - functions/crates/lpe-storage/src/schema_contract/collaboration_changes_and_tombstones_are_object_level
  - functions/crates/lpe-storage/src/schema_contract/mapi_identity_mapping_is_store_backed
  - functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable
  - functions/crates/lpe-storage/src/schema_contract/deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads
  - functions/crates/lpe-storage/src/schema_contract/mapi_associated_config_messages_are_bounded_mapi_only_state
  - functions/crates/lpe-storage/src/schema_contract/mapi_named_properties_and_custom_values_are_durable
  - functions/crates/lpe-storage/src/schema_contract/blob_placement_metadata_is_tenant_domain_and_blob_safe
  - functions/crates/lpe-storage/src/schema_contract/storage_policy_assignments_capture_milestone_five_scope_contract
  - functions/crates/lpe-storage/src/schema_contract/admin_settings_and_auth_runtime_tables_exist_in_core_schema
  - functions/crates/lpe-storage/src/schema_contract/admin_workspace_and_pst_use_v2_mailbox_membership_schema
  - functions/crates/lpe-storage/src/schema_contract/conversation_actions_are_canonical_fai_state
  - functions/crates/lpe-storage/src/schema_contract/mailbox_rules_are_canonical_sieve_scripts_with_replay
  - functions/crates/lpe-storage/src/schema_contract/blob_and_message_lifecycle_metadata_support_cleanup_guards
  - functions/crates/lpe-storage/src/schema_contract/imap_uid_state_is_mailbox_scoped_without_global_sequence
  - functions/crates/lpe-storage/src/schema_contract/mailbox_hierarchy_and_subscriptions_are_canonical_storage
  - functions/crates/lpe-storage/src/schema_contract/runtime_access_paths_have_scaling_indexes
  - functions/crates/lpe-storage/src/schema_contract/activesync_sync_state_uses_v2_cursor_table
---

# Signature

`fn assert_schema_contains_all(needles: &[&str])`

# Called by

- [collaboration_objects_have_canonical_projection_fields](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_objects_have_canonical_projection_fields.md)
- [public_folder_schema_uses_canonical_tables_permissions_and_replay](../../../../../functions/crates/lpe-storage/src/schema_contract/public_folder_schema_uses_canonical_tables_permissions_and_replay.md)
- [ews_compatibility_gap_models_are_canonical_sql_state](../../../../../functions/crates/lpe-storage/src/schema_contract/ews_compatibility_gap_models_are_canonical_sql_state.md)
- [calendar_event_attachments_use_canonical_event_and_blob_tables](../../../../../functions/crates/lpe-storage/src/schema_contract/calendar_event_attachments_use_canonical_event_and_blob_tables.md)
- [notes_journal_and_reminders_stay_canonical](../../../../../functions/crates/lpe-storage/src/schema_contract/notes_journal_and_reminders_stay_canonical.md)
- [collaboration_rights_are_canonical_and_same_tenant](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_rights_are_canonical_and_same_tenant.md)
- [collaboration_changes_and_tombstones_are_object_level](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_changes_and_tombstones_are_object_level.md)
- [mapi_identity_mapping_is_store_backed](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_identity_mapping_is_store_backed.md)
- [mapi_local_replica_ranges_and_deleted_item_list_are_durable](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable.md)
- [deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads](../../../../../functions/crates/lpe-storage/src/schema_contract/deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads.md)
- [mapi_associated_config_messages_are_bounded_mapi_only_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_associated_config_messages_are_bounded_mapi_only_state.md)
- [mapi_named_properties_and_custom_values_are_durable](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_named_properties_and_custom_values_are_durable.md)
- [blob_placement_metadata_is_tenant_domain_and_blob_safe](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_placement_metadata_is_tenant_domain_and_blob_safe.md)
- [storage_policy_assignments_capture_milestone_five_scope_contract](../../../../../functions/crates/lpe-storage/src/schema_contract/storage_policy_assignments_capture_milestone_five_scope_contract.md)
- [admin_settings_and_auth_runtime_tables_exist_in_core_schema](../../../../../functions/crates/lpe-storage/src/schema_contract/admin_settings_and_auth_runtime_tables_exist_in_core_schema.md)
- [admin_workspace_and_pst_use_v2_mailbox_membership_schema](../../../../../functions/crates/lpe-storage/src/schema_contract/admin_workspace_and_pst_use_v2_mailbox_membership_schema.md)
- [conversation_actions_are_canonical_fai_state](../../../../../functions/crates/lpe-storage/src/schema_contract/conversation_actions_are_canonical_fai_state.md)
- [mailbox_rules_are_canonical_sieve_scripts_with_replay](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_rules_are_canonical_sieve_scripts_with_replay.md)
- [blob_and_message_lifecycle_metadata_support_cleanup_guards](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_and_message_lifecycle_metadata_support_cleanup_guards.md)
- [imap_uid_state_is_mailbox_scoped_without_global_sequence](../../../../../functions/crates/lpe-storage/src/schema_contract/imap_uid_state_is_mailbox_scoped_without_global_sequence.md)
- [mailbox_hierarchy_and_subscriptions_are_canonical_storage](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_hierarchy_and_subscriptions_are_canonical_storage.md)
- [runtime_access_paths_have_scaling_indexes](../../../../../functions/crates/lpe-storage/src/schema_contract/runtime_access_paths_have_scaling_indexes.md)
- [activesync_sync_state_uses_v2_cursor_table](../../../../../functions/crates/lpe-storage/src/schema_contract/activesync_sync_state_uses_v2_cursor_table.md)