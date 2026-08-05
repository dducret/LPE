---
type: Rust Function
title: only_sync_collection
resource: crates/lpe-activesync/src/tests.rs#L2038-L2050
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/tests/unsupported_sync_child_command_returns_protocol_status
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
  - functions/crates/lpe-activesync/src/tests/restart_safe_no_change_sync_keeps_persisted_key_usable
  - functions/crates/lpe-activesync/src/tests/unknown_sync_key_is_rejected_with_invalid_sync_key_status
  - functions/crates/lpe-activesync/src/tests/expired_sync_key_is_cleaned_up_and_rejected
  - functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required
  - functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields
  - functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees
  - functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_projection_includes_supported_application_data
---

# Signature

`fn only_sync_collection<'a>(sync: &'a WbxmlNode, collection_id: &str) -> &'a WbxmlNode`

# Calls

- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [unsupported_sync_child_command_returns_protocol_status](../../../../../functions/crates/lpe-activesync/src/tests/unsupported_sync_child_command_returns_protocol_status.md)
- [sync_change_updates_read_state_and_round_trips](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips.md)
- [sync_change_updates_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
- [restart_safe_no_change_sync_keeps_persisted_key_usable](../../../../../functions/crates/lpe-activesync/src/tests/restart_safe_no_change_sync_keeps_persisted_key_usable.md)
- [unknown_sync_key_is_rejected_with_invalid_sync_key_status](../../../../../functions/crates/lpe-activesync/src/tests/unknown_sync_key_is_rejected_with_invalid_sync_key_status.md)
- [expired_sync_key_is_cleaned_up_and_rejected](../../../../../functions/crates/lpe-activesync/src/tests/expired_sync_key_is_cleaned_up_and_rejected.md)
- [superseded_incomplete_sync_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected.md)
- [hierarchy_change_after_existing_sync_returns_folder_sync_required](../../../../../functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required.md)
- [sync_projects_email_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state.md)
- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)
- [sync_contact_and_calendar_projection_includes_supported_application_data](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_projection_includes_supported_application_data.md)