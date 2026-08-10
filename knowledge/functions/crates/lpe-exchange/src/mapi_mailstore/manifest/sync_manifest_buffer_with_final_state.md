---
type: Rust Function
title: sync_manifest_buffer_with_final_state
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L577-L617
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_omits_content_activity_count_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_hierarchy_sync_projects_outlook_special_folder_display_names
---

# Signature

`pub(crate) fn sync_manifest_buffer_with_final_state( mailbox_guid: Uuid, sync_type: u8, sync_flags: u16, sync_extra_flags: u32, sync_property_tags: &[u32], folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], deleted_message_ids: &[u64], parent_context_mailboxes: &[JmapMailbox], state_mailboxes: &[JmapMailbox], state_emails: &[JmapEmail], state_attachment_facts: &[MessageAttachmentSyncFacts], aggregate_emails: &[JmapEmail], aggregate_attachment_facts: &[MessageAttachmentSyncFacts], _final_change_sequence: u64, ) -> Vec<u8>`

# Calls

- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)

# Called by

- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape.md)
- [hierarchy_transfer_calendar_includes_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id.md)
- [hierarchy_transfer_inbox_includes_calendar_identification_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id.md)
- [hierarchy_transfer_respects_entry_id_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_string8_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion.md)
- [hierarchy_sync_omits_content_activity_count_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_omits_content_activity_count_properties.md)
- [hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts.md)
- [mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy.md)
- [mapi_hierarchy_sync_projects_outlook_special_folder_display_names](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_hierarchy_sync_projects_outlook_special_folder_display_names.md)