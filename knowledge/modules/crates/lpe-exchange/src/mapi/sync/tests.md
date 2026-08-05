---
type: Rust Module
title: tests
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L1-L2374
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-mapi-rop-roprequest
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [sync_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/mailbox.md)
- [assert_associated_fai_core_payload](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_associated_fai_core_payload.md)
- [assert_has_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_has_tags.md)
- [associated_content_sync_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [associated_content_sync_buffer_with_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags.md)
- [assert_fai_boundary_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_fai_boundary_summary.md)
- [persisted_inbox_associated_configs](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_inbox_associated_configs.md)
- [persisted_common_views_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_common_views_shortcuts.md)
- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [associated_config_fai_no_foreign_identifiers_uses_local_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key.md)
- [appointment_fast_transfer_named_lid_includes_property_definition](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/appointment_fast_transfer_named_lid_includes_property_definition.md)
- [import_rop_success_responses_return_zero_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/import_rop_success_responses_return_zero_object_ids.md)
- [fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata.md)
- [fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata.md)
- [hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids.md)
- [hierarchy_sync_mailboxes_include_custom_sync_root](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_include_custom_sync_root.md)
- [calendar_sync_object_projects_canonical_attachment_presence](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_sync_object_projects_canonical_attachment_presence.md)
- [calendar_special_content_sync_advertises_appointment_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_special_content_sync_advertises_appointment_objects.md)
- [collaboration_default_views_are_not_synthetic_fai_sync_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/collaboration_default_views_are_not_synthetic_fai_sync_objects.md)
- [hierarchy_sync_mailboxes_deduplicate_outlook_special_roles](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_outlook_special_roles.md)
- [hierarchy_scope_places_reminders_under_root_not_ipm_subtree](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_scope_places_reminders_under_root_not_ipm_subtree.md)
- [hierarchy_scope_places_contacts_search_under_search_not_ipm_subtree](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_scope_places_contacts_search_under_search_not_ipm_subtree.md)
- [ipm_hierarchy_runtime_uses_outlook_safe_folder_projection](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_runtime_uses_outlook_safe_folder_projection.md)
- [ipm_hierarchy_state_matches_emitted_folder_projection](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_state_matches_emitted_folder_projection.md)
- [ipm_hierarchy_scope_includes_durable_hidden_special_folder_alias_targets](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_scope_includes_durable_hidden_special_folder_alias_targets.md)
- [common_views_shortcut_sync_uses_account_bound_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids.md)
- [common_views_shortcut_sync_does_not_emit_materialized_mail_header](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header.md)
- [common_views_group_header_sync_includes_group_identity_without_target](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [fast_transfer_message_children_follow_level_and_property_tag_semantics](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_message_children_follow_level_and_property_tag_semantics.md)
- [special_message_general_properties_follow_fast_transfer_property_filters](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters.md)
- [navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id.md)
- [fast_transfer_manifest_rejects_unbacked_common_views_shortcut](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_unbacked_common_views_shortcut.md)
- [fast_transfer_manifest_rejects_unpersisted_common_views_named_view](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_unpersisted_common_views_named_view.md)
- [common_view_named_view_sync_projects_canonical_descriptor_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties.md)
- [fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder.md)
- [local_freebusy_direct_copy_projects_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id.md)

# Imports

- `super::*`
- `crate::mapi::rop::RopRequest`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)