---
type: Rust Function
title: handle_sync_node
resource: crates/lpe-activesync/src/tests.rs#L2135-L2150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  called_by:
  - functions/crates/lpe-activesync/src/tests/sync_missing_and_invalid_collection_ids_return_status_nodes
  - functions/crates/lpe-activesync/src/tests/unsupported_sync_child_command_returns_protocol_status
  - functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders
  - functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime
  - functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields
  - functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees
---

# Signature

`async fn handle_sync_node(service: &ActiveSyncService<FakeStore>, node: WbxmlNode) -> WbxmlNode`

# Calls

- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)

# Called by

- [sync_missing_and_invalid_collection_ids_return_status_nodes](../../../../../functions/crates/lpe-activesync/src/tests/sync_missing_and_invalid_collection_ids_return_status_nodes.md)
- [unsupported_sync_child_command_returns_protocol_status](../../../../../functions/crates/lpe-activesync/src/tests/unsupported_sync_child_command_returns_protocol_status.md)
- [move_items_moves_message_between_canonical_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders.md)
- [sync_delete_moves_message_to_trash_by_default](../../../../../functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default.md)
- [sync_change_updates_read_state_and_round_trips](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips.md)
- [sync_change_updates_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [sync_projects_email_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state.md)
- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)