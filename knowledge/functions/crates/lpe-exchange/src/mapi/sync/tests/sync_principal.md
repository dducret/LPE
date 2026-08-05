---
type: Rust Function
title: sync_principal
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L4-L13
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_special_content_sync_advertises_appointment_objects
  - functions/crates/lpe-exchange/src/mapi/sync/tests/collaboration_default_views_are_not_synthetic_fai_sync_objects
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters
  - functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id
---

# Signature

`fn sync_principal(account_id: Uuid) -> AccountPrincipal`

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [associated_config_fai_no_foreign_identifiers_uses_local_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key.md)
- [calendar_special_content_sync_advertises_appointment_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_special_content_sync_advertises_appointment_objects.md)
- [collaboration_default_views_are_not_synthetic_fai_sync_objects](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/collaboration_default_views_are_not_synthetic_fai_sync_objects.md)
- [common_views_shortcut_sync_uses_account_bound_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids.md)
- [common_views_shortcut_sync_does_not_emit_materialized_mail_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header.md)
- [common_views_group_header_sync_includes_group_identity_without_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [special_message_general_properties_follow_fast_transfer_property_filters](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters.md)
- [navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id.md)
- [fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder.md)
- [local_freebusy_direct_copy_projects_account_scoped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id.md)