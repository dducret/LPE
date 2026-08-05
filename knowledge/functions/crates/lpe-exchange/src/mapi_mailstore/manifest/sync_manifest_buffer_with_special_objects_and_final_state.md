---
type: Rust Function
title: sync_manifest_buffer_with_special_objects_and_final_state
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L619-L662
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_includes_special_folder_message_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_property_excludes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_excludes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_includes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_respects_normal_and_fai_scope_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers
---

# Signature

`pub(crate) fn sync_manifest_buffer_with_special_objects_and_final_state( mailbox_guid: Uuid, sync_type: u8, sync_flags: u16, sync_extra_flags: u32, sync_property_tags: &[u32], folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], special_objects: &[SpecialMessageSyncFact], deleted_message_ids: &[u64], parent_context_mailboxes: &[JmapMailbox], state_mailboxes: &[JmapMailbox], state_emails: &[JmapEmail], state_attachment_facts: &[MessageAttachmentSyncFacts], state_special_objects: &[SpecialMessageSyncFact], aggregate_emails: &[JmapEmail], aggregate_attachment_facts: &[MessageAttachmentSyncFacts], _final_change_sequence: u64, ) -> Vec<u8>`

# Calls

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions.md)

# Called by

- [associated_content_sync_buffer_with_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [sync_manifest_buffer_with_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)
- [microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes.md)
- [microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property.md)
- [content_sync_manifest_includes_special_folder_message_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_includes_special_folder_message_objects.md)
- [microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example.md)
- [content_sync_manifest_starts_fai_message_before_item_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties.md)
- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)
- [content_sync_manifest_applies_property_excludes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_excludes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_includes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_includes_to_special_objects.md)
- [content_sync_manifest_respects_normal_and_fai_scope_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_respects_normal_and_fai_scope_flags.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)