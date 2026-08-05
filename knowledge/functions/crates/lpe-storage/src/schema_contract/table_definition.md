---
type: Rust Function
title: table_definition
resource: crates/lpe-storage/src/schema_contract.rs#L84-L91
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/mailbox_messages_persist_outlook_followup_state
  - functions/crates/lpe-storage/src/schema_contract/replay_logs_tombstones_and_cursors_have_structural_constraints
  - functions/crates/lpe-storage/src/schema_contract/mapi_identity_mapping_is_store_backed
  - functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable
  - functions/crates/lpe-storage/src/schema_contract/deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads
  - functions/crates/lpe-storage/src/schema_contract/mapi_navigation_shortcuts_persist_group_header_links
  - functions/crates/lpe-storage/src/schema_contract/mapi_associated_config_messages_are_bounded_mapi_only_state
  - functions/crates/lpe-storage/src/schema_contract/mapi_named_properties_and_custom_values_are_durable
  - functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings
  - functions/crates/lpe-storage/src/schema_contract/mapi_folder_profile_properties_are_bounded_profile_state
  - functions/crates/lpe-storage/src/schema_contract/mapi_special_folder_aliases_are_bounded_protocol_identity_metadata
  - functions/crates/lpe-storage/src/schema_contract/blob_placement_metadata_is_tenant_domain_and_blob_safe
  - functions/crates/lpe-storage/src/schema_contract/blob_references_enforce_kind_and_attachment_ownership
  - functions/crates/lpe-storage/src/schema_contract/storage_policy_assignments_capture_milestone_five_scope_contract
  - functions/crates/lpe-storage/src/schema_contract/audit_events_support_platform_and_tenant_admin_policy_events
  - functions/crates/lpe-storage/src/schema_contract/admin_settings_and_auth_runtime_tables_exist_in_core_schema
  - functions/crates/lpe-storage/src/schema_contract/mailbox_identity_schema_has_generated_normalized_address_helpers
  - functions/crates/lpe-storage/src/schema_contract/account_creation_allocates_canonical_send_identity_rows
  - functions/crates/lpe-storage/src/schema_contract/mailbox_schema_allows_canonical_outlook_compatibility_mail_roles
  - functions/crates/lpe-storage/src/schema_contract/search_folder_schema_persists_exchange_builtin_definitions
  - functions/crates/lpe-storage/src/schema_contract/conversation_actions_are_canonical_fai_state
  - functions/crates/lpe-storage/src/schema_contract/recipient_suggestions_are_owner_scoped_private_ranked_signals
  - functions/crates/lpe-storage/src/schema_contract/recipient_suggestions_contact_delete_clears_only_contact_id
  - functions/crates/lpe-storage/src/schema_contract/contact_book_schema_allows_outlook_compatibility_roles
  - functions/crates/lpe-storage/src/schema_contract/blob_migration_jobs_capture_milestone_three_worker_contract
  - functions/crates/lpe-storage/src/schema_contract/blob_and_message_lifecycle_metadata_support_cleanup_guards
  - functions/crates/lpe-storage/src/schema_contract/recoverable_items_are_canonical_lifecycle_state
  - functions/crates/lpe-storage/src/schema_contract/mailbox_moves_create_target_membership_and_tombstone_source_uid
  - functions/crates/lpe-storage/src/schema_contract/bcc_is_absent_from_search_log_cursor_and_ai_projection_tables
  - functions/crates/lpe-storage/src/schema_contract/protocol_cursor_tables_do_not_store_canonical_content
---

# Signature

`fn table_definition(table_name: &str) -> &str`

# Called by

- [mailbox_messages_persist_outlook_followup_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_messages_persist_outlook_followup_state.md)
- [replay_logs_tombstones_and_cursors_have_structural_constraints](../../../../../functions/crates/lpe-storage/src/schema_contract/replay_logs_tombstones_and_cursors_have_structural_constraints.md)
- [mapi_identity_mapping_is_store_backed](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_identity_mapping_is_store_backed.md)
- [mapi_local_replica_ranges_and_deleted_item_list_are_durable](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable.md)
- [deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads](../../../../../functions/crates/lpe-storage/src/schema_contract/deleted_calendar_events_remain_canonical_and_are_hidden_from_active_reads.md)
- [mapi_navigation_shortcuts_persist_group_header_links](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_navigation_shortcuts_persist_group_header_links.md)
- [mapi_associated_config_messages_are_bounded_mapi_only_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_associated_config_messages_are_bounded_mapi_only_state.md)
- [mapi_named_properties_and_custom_values_are_durable](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_named_properties_and_custom_values_are_durable.md)
- [mapi_profile_settings_are_canonical_account_settings](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings.md)
- [mapi_folder_profile_properties_are_bounded_profile_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_folder_profile_properties_are_bounded_profile_state.md)
- [mapi_special_folder_aliases_are_bounded_protocol_identity_metadata](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_special_folder_aliases_are_bounded_protocol_identity_metadata.md)
- [blob_placement_metadata_is_tenant_domain_and_blob_safe](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_placement_metadata_is_tenant_domain_and_blob_safe.md)
- [blob_references_enforce_kind_and_attachment_ownership](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_references_enforce_kind_and_attachment_ownership.md)
- [storage_policy_assignments_capture_milestone_five_scope_contract](../../../../../functions/crates/lpe-storage/src/schema_contract/storage_policy_assignments_capture_milestone_five_scope_contract.md)
- [audit_events_support_platform_and_tenant_admin_policy_events](../../../../../functions/crates/lpe-storage/src/schema_contract/audit_events_support_platform_and_tenant_admin_policy_events.md)
- [admin_settings_and_auth_runtime_tables_exist_in_core_schema](../../../../../functions/crates/lpe-storage/src/schema_contract/admin_settings_and_auth_runtime_tables_exist_in_core_schema.md)
- [mailbox_identity_schema_has_generated_normalized_address_helpers](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_identity_schema_has_generated_normalized_address_helpers.md)
- [account_creation_allocates_canonical_send_identity_rows](../../../../../functions/crates/lpe-storage/src/schema_contract/account_creation_allocates_canonical_send_identity_rows.md)
- [mailbox_schema_allows_canonical_outlook_compatibility_mail_roles](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_schema_allows_canonical_outlook_compatibility_mail_roles.md)
- [search_folder_schema_persists_exchange_builtin_definitions](../../../../../functions/crates/lpe-storage/src/schema_contract/search_folder_schema_persists_exchange_builtin_definitions.md)
- [conversation_actions_are_canonical_fai_state](../../../../../functions/crates/lpe-storage/src/schema_contract/conversation_actions_are_canonical_fai_state.md)
- [recipient_suggestions_are_owner_scoped_private_ranked_signals](../../../../../functions/crates/lpe-storage/src/schema_contract/recipient_suggestions_are_owner_scoped_private_ranked_signals.md)
- [recipient_suggestions_contact_delete_clears_only_contact_id](../../../../../functions/crates/lpe-storage/src/schema_contract/recipient_suggestions_contact_delete_clears_only_contact_id.md)
- [contact_book_schema_allows_outlook_compatibility_roles](../../../../../functions/crates/lpe-storage/src/schema_contract/contact_book_schema_allows_outlook_compatibility_roles.md)
- [blob_migration_jobs_capture_milestone_three_worker_contract](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_migration_jobs_capture_milestone_three_worker_contract.md)
- [blob_and_message_lifecycle_metadata_support_cleanup_guards](../../../../../functions/crates/lpe-storage/src/schema_contract/blob_and_message_lifecycle_metadata_support_cleanup_guards.md)
- [recoverable_items_are_canonical_lifecycle_state](../../../../../functions/crates/lpe-storage/src/schema_contract/recoverable_items_are_canonical_lifecycle_state.md)
- [mailbox_moves_create_target_membership_and_tombstone_source_uid](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_moves_create_target_membership_and_tombstone_source_uid.md)
- [bcc_is_absent_from_search_log_cursor_and_ai_projection_tables](../../../../../functions/crates/lpe-storage/src/schema_contract/bcc_is_absent_from_search_log_cursor_and_ai_projection_tables.md)
- [protocol_cursor_tables_do_not_store_canonical_content](../../../../../functions/crates/lpe-storage/src/schema_contract/protocol_cursor_tables_do_not_store_canonical_content.md)