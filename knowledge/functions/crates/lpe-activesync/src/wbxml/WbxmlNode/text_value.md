---
type: Rust Method
title: text_value
resource: crates/lpe-activesync/src/wbxml.rs#L60-L62
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/message/merged_draft_input
  - functions/crates/lpe-activesync/src/message/draft_input_from_application_data
  - functions/crates/lpe-activesync/src/message/field_text
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_contact_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands
  - functions/crates/lpe-activesync/src/service/application_data/body_text
  - functions/crates/lpe-activesync/src/service/application_data/attendees_from_nodes
  - functions/crates/lpe-activesync/src/service/body_preferences/options_body_preference
  - functions/crates/lpe-activesync/src/service/body_preferences/collection_deletes_as_moves
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_settings_from_request
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
  - functions/crates/lpe-activesync/src/service/search/search_query_text
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message
  - functions/crates/lpe-activesync/src/snapshot/collection_window_size
  - functions/crates/lpe-activesync/src/snapshot/require_collection_id
  - functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
  - functions/crates/lpe-activesync/src/tests/collection_sync_key
  - functions/crates/lpe-activesync/src/tests/only_sync_collection
  - functions/crates/lpe-activesync/src/tests/status_value
  - functions/crates/lpe-activesync/src/tests/folder_add
  - functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2
  - functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime
  - functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections
  - functions/crates/lpe-activesync/src/tests/stale_folder_sync_key_is_rejected_after_completed_round
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders
  - functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
  - functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required
  - functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state
  - functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections
  - functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields
  - functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees
---

# Signature

`pub(crate) fn text_value(&self) -> &str`

# Called by

- [merged_draft_input](../../../../../../functions/crates/lpe-activesync/src/message/merged_draft_input.md)
- [draft_input_from_application_data](../../../../../../functions/crates/lpe-activesync/src/message/draft_input_from_application_data.md)
- [field_text](../../../../../../functions/crates/lpe-activesync/src/message/field_text.md)
- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)
- [apply_draft_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)
- [apply_contact_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_contact_sync_commands.md)
- [apply_calendar_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands.md)
- [body_text](../../../../../../functions/crates/lpe-activesync/src/service/application_data/body_text.md)
- [attendees_from_nodes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/attendees_from_nodes.md)
- [options_body_preference](../../../../../../functions/crates/lpe-activesync/src/service/body_preferences/options_body_preference.md)
- [collection_deletes_as_moves](../../../../../../functions/crates/lpe-activesync/src/service/body_preferences/collection_deletes_as_moves.md)
- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [get_item_estimate_response](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
- [handle_item_operations_fetch](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [handle_move_item](../../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)
- [ping_settings_from_request](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_settings_from_request.md)
- [handle_provision](../../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)
- [handle_search](../../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)
- [search_query_text](../../../../../../functions/crates/lpe-activesync/src/service/search/search_query_text.md)
- [handle_send_mail](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)
- [resolve_source_message](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message.md)
- [collection_window_size](../../../../../../functions/crates/lpe-activesync/src/snapshot/collection_window_size.md)
- [require_collection_id](../../../../../../functions/crates/lpe-activesync/src/snapshot/require_collection_id.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)
- [collection_sync_key](../../../../../../functions/crates/lpe-activesync/src/tests/collection_sync_key.md)
- [only_sync_collection](../../../../../../functions/crates/lpe-activesync/src/tests/only_sync_collection.md)
- [status_value](../../../../../../functions/crates/lpe-activesync/src/tests/status_value.md)
- [folder_add](../../../../../../functions/crates/lpe-activesync/src/tests/folder_add.md)
- [search_malformed_range_returns_store_status_2](../../../../../../functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [folder_sync_returns_mail_and_collaboration_collections](../../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections.md)
- [stale_folder_sync_key_is_rejected_after_completed_round](../../../../../../functions/crates/lpe-activesync/src/tests/stale_folder_sync_key_is_rejected_after_completed_round.md)
- [folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key](../../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_update_renames_custom_mail_folder](../../../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
- [folder_delete_deletes_custom_mail_folder](../../../../../../functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
- [folder_mutation_with_stale_hierarchy_key_is_rejected](../../../../../../functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
- [hierarchy_change_after_existing_sync_returns_folder_sync_required](../../../../../../functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required.md)
- [ping_reconnects_after_service_restart_using_persisted_sync_state](../../../../../../functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state.md)
- [ping_detects_changes_across_multiple_monitored_collections](../../../../../../functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections.md)
- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)