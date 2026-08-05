---
type: Rust Function
title: collection_sync_key
resource: crates/lpe-activesync/src/tests.rs#L1864-L1878
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders
  - functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
  - functions/crates/lpe-activesync/src/tests/sync_key_zero_primes_then_returns_paged_more_available_changes
  - functions/crates/lpe-activesync/src/tests/get_item_estimate_returns_pending_sync_count
  - functions/crates/lpe-activesync/src/tests/stable_sync_does_not_reload_full_email_payloads_without_changes
  - functions/crates/lpe-activesync/src/tests/sync_key_stays_usable_for_new_changes_after_a_stable_round
  - functions/crates/lpe-activesync/src/tests/stale_sync_key_is_rejected_after_a_completed_round
  - functions/crates/lpe-activesync/src/tests/restart_safe_no_change_sync_keeps_persisted_key_usable
  - functions/crates/lpe-activesync/src/tests/expired_sync_key_is_cleaned_up_and_rejected
  - functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required
  - functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields
  - functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees
  - functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_projection_includes_supported_application_data
---

# Signature

`fn collection_sync_key(sync: &WbxmlNode, collection_id: &str) -> String`

# Calls

- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [move_items_moves_message_between_canonical_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders.md)
- [sync_delete_moves_message_to_trash_by_default](../../../../../functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default.md)
- [sync_change_updates_read_state_and_round_trips](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips.md)
- [sync_change_updates_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
- [sync_key_zero_primes_then_returns_paged_more_available_changes](../../../../../functions/crates/lpe-activesync/src/tests/sync_key_zero_primes_then_returns_paged_more_available_changes.md)
- [get_item_estimate_returns_pending_sync_count](../../../../../functions/crates/lpe-activesync/src/tests/get_item_estimate_returns_pending_sync_count.md)
- [stable_sync_does_not_reload_full_email_payloads_without_changes](../../../../../functions/crates/lpe-activesync/src/tests/stable_sync_does_not_reload_full_email_payloads_without_changes.md)
- [sync_key_stays_usable_for_new_changes_after_a_stable_round](../../../../../functions/crates/lpe-activesync/src/tests/sync_key_stays_usable_for_new_changes_after_a_stable_round.md)
- [stale_sync_key_is_rejected_after_a_completed_round](../../../../../functions/crates/lpe-activesync/src/tests/stale_sync_key_is_rejected_after_a_completed_round.md)
- [restart_safe_no_change_sync_keeps_persisted_key_usable](../../../../../functions/crates/lpe-activesync/src/tests/restart_safe_no_change_sync_keeps_persisted_key_usable.md)
- [expired_sync_key_is_cleaned_up_and_rejected](../../../../../functions/crates/lpe-activesync/src/tests/expired_sync_key_is_cleaned_up_and_rejected.md)
- [superseded_incomplete_sync_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected.md)
- [hierarchy_change_after_existing_sync_returns_folder_sync_required](../../../../../functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required.md)
- [sync_projects_email_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state.md)
- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)
- [sync_contact_and_calendar_projection_includes_supported_application_data](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_projection_includes_supported_application_data.md)