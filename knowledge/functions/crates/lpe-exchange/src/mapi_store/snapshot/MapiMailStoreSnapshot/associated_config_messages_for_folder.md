---
type: Rust Method
title: associated_config_messages_for_folder
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1354-L1375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_sync_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_identity_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key
  - functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_inbox_compact_named_view_remains_canonical
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row
  - functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_configs_keep_outlook_migration_markers_visible
  - functions/crates/lpe-exchange/src/mapi_store/tests/quick_step_settings_do_not_invent_custom_action_state
  - functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai
  - functions/crates/lpe-exchange/src/mapi_store/tests/dynamic_contact_folder_exposes_only_persisted_associated_config
  - functions/crates/lpe-exchange/src/mapi_store/tests/mailbox_backed_contact_folder_does_not_invent_osc_contact_sync
  - functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
---

# Signature

`pub(crate) fn associated_config_messages_for_folder( &self, folder_id: u64, ) -> Vec<MapiAssociatedConfigMessage>`

# Calls

- [outlook_inbox_associated_config_sync_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [has_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows.md)
- [associated_table_rows_with_lookup_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)
- [canonical_common_views_fai_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages.md)
- [associated_config_sync_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_sync_messages_for_folder.md)
- [associated_config_message_for_identity_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_identity_id.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)
- [associated_config_message_for_folder_and_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key.md)
- [inbox_associated_configs_do_not_emit_unpersisted_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults.md)
- [empty_persisted_inbox_compact_named_view_remains_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_inbox_compact_named_view_remains_canonical.md)
- [empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row.md)
- [stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row.md)
- [associated_config_sync_messages_use_persisted_rows_before_narrow_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults.md)
- [associated_configs_keep_outlook_migration_markers_visible](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_configs_keep_outlook_migration_markers_visible.md)
- [quick_step_settings_do_not_invent_custom_action_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/quick_step_settings_do_not_invent_custom_action_state.md)
- [contacts_project_exactly_the_persisted_contact_link_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai.md)
- [dynamic_contact_folder_exposes_only_persisted_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/dynamic_contact_folder_exposes_only_persisted_associated_config.md)
- [mailbox_backed_contact_folder_does_not_invent_osc_contact_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/mailbox_backed_contact_folder_does_not_invent_osc_contact_sync.md)
- [distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)