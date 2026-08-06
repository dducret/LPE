---
type: Rust Module
title: tests
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L1-L3534
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-mapi-properties-default-wlink-group-uuid
  - external/lpe-storage-accessiblecontact-collaborationcollection-collaborationrights-jmapemailaddress-jmapemailmailboxstate
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [exchange_builtin_excluded_folder_roles](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/exchange_builtin_excluded_folder_roles.md)
- [test_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_mailbox.md)
- [test_email](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_email.md)
- [test_accessible_event](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_accessible_event.md)
- [test_mapi_event](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_mapi_event.md)
- [event_lookup_rejects_another_principals_cached_mid](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/event_lookup_rejects_another_principals_cached_mid.md)
- [exact_event_mid_wins_over_another_events_foreign_cached_alias](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias.md)
- [contact_commit_times_override_the_durable_contact_identity_timestamp](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contact_commit_times_override_the_durable_contact_identity_timestamp.md)
- [content_table_window_emails_reuses_wider_window_slice](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_reuses_wider_window_slice.md)
- [content_table_window_emails_skips_insufficient_containing_window](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_skips_insufficient_containing_window.md)
- [content_table_window_emails_containing_skips_incomplete_window](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_skips_incomplete_window.md)
- [content_table_window_emails_containing_prefers_boundary_window](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_boundary_window.md)
- [content_table_window_emails_containing_prefers_longer_tail_window](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_longer_tail_window.md)
- [content_table_total_survives_total_only_window_without_rows](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_total_survives_total_only_window_without_rows.md)
- [advertised_special_mailbox_roles_have_reserved_mapi_counters](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/advertised_special_mailbox_roles_have_reserved_mapi_counters.md)
- [inbox_associated_configs_do_not_emit_unpersisted_defaults](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults.md)
- [empty_persisted_inbox_compact_named_view_remains_canonical](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_inbox_compact_named_view_remains_canonical.md)
- [empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row.md)
- [stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row.md)
- [associated_config_sync_messages_use_persisted_rows_before_narrow_defaults](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults.md)
- [empty_rule_organizer_placeholder_is_not_modeled_state](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_rule_organizer_placeholder_is_not_modeled_state.md)
- [associated_configs_keep_outlook_migration_markers_visible](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_configs_keep_outlook_migration_markers_visible.md)
- [quick_step_settings_do_not_invent_custom_action_state](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/quick_step_settings_do_not_invent_custom_action_state.md)
- [empty_contact_folders_expose_no_synthetic_associated_config](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_contact_folders_expose_no_synthetic_associated_config.md)
- [contacts_project_exactly_the_persisted_contact_link_fai](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai.md)
- [dynamic_contact_folder_exposes_only_persisted_associated_config](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/dynamic_contact_folder_exposes_only_persisted_associated_config.md)
- [mailbox_backed_contact_folder_does_not_invent_osc_contact_sync](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/mailbox_backed_contact_folder_does_not_invent_osc_contact_sync.md)
- [mailbox_backed_suggested_contacts_does_not_invent_osc_contact_sync](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/mailbox_backed_suggested_contacts_does_not_invent_osc_contact_sync.md)
- [associated_config_identity_only_placeholder_does_not_open_without_backing_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_identity_only_placeholder_does_not_open_without_backing_message.md)
- [distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection.md)
- [modeled_virtual_associated_config_identity_opens_via_dynamic_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/modeled_virtual_associated_config_identity_opens_via_dynamic_id.md)
- [empty_conversation_action_settings_exposes_no_synthetic_rows](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_conversation_action_settings_exposes_no_synthetic_rows.md)
- [empty_common_views_exposes_no_synthetic_fai](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_common_views_exposes_no_synthetic_fai.md)
- [folder_default_named_views_do_not_materialize_without_persisted_selection](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/folder_default_named_views_do_not_materialize_without_persisted_selection.md)
- [persisted_default_named_view_requires_one_matching_folder_view](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/persisted_default_named_view_requires_one_matching_folder_view.md)
- [legacy_default_named_view_alias_does_not_materialize_a_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/legacy_default_named_view_alias_does_not_materialize_a_message.md)
- [sent_common_views_default_view_does_not_materialize_folder_local_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/sent_common_views_default_view_does_not_materialize_folder_local_message.md)
- [folder_local_default_named_view_ids_are_not_openable](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/folder_local_default_named_view_ids_are_not_openable.md)
- [common_views_skips_search_folder_definition_without_protocol_blob](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_skips_search_folder_definition_without_protocol_blob.md)
- [common_views_projects_search_folder_definition_with_protocol_blob](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_search_folder_definition_with_protocol_blob.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)
- [common_views_preserves_persisted_calendar_group_and_shortcut_identity](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_persisted_calendar_group_and_shortcut_identity.md)
- [common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties.md)
- [common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links.md)
- [common_views_projects_persisted_default_mail_favorites_in_startup_table](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_persisted_default_mail_favorites_in_startup_table.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)
- [snapshot_projects_canonical_mailbox_message_and_attachment_ids](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_mailbox_message_and_attachment_ids.md)
- [snapshot_projects_outlook_contact_books_into_fixed_mapi_folders](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_outlook_contact_books_into_fixed_mapi_folders.md)
- [collaboration_folder_identity_requests_cover_custom_and_shared_collections](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/collaboration_folder_identity_requests_cover_custom_and_shared_collections.md)
- [snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders.md)
- [snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded.md)
- [snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders.md)
- [snapshot_carries_persisted_search_folder_definitions](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_carries_persisted_search_folder_definitions.md)
- [snapshot_resolves_tracked_mail_processing_by_advertised_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_resolves_tracked_mail_processing_by_advertised_folder_id.md)
- [snapshot_projects_user_saved_search_folder_as_mapi_folder](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_user_saved_search_folder_as_mapi_folder.md)
- [snapshot_deduplicates_user_saved_search_folder_projection_by_name](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_deduplicates_user_saved_search_folder_projection_by_name.md)
- [snapshot_ignores_blank_mapi_bounded_user_saved_search_folder](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_ignores_blank_mapi_bounded_user_saved_search_folder.md)
- [snapshot_projects_canonical_tasks_into_todo_search_results](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_tasks_into_todo_search_results.md)
- [snapshot_projects_followup_mail_into_todo_search_results](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_followup_mail_into_todo_search_results.md)
- [snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results.md)
- [snapshot_projects_reminders_as_underlying_calendar_and_task_links](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_reminders_as_underlying_calendar_and_task_links.md)
- [snapshot_projects_computed_delegate_freebusy_messages](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_computed_delegate_freebusy_messages.md)

# Imports

- `super::*`
- `crate::mapi::properties::default_wlink_group_uuid`
- `lpe_storage::{
    AccessibleContact, CollaborationCollection, CollaborationRights, JmapEmailAddress,
    JmapEmailMailboxState,
}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)