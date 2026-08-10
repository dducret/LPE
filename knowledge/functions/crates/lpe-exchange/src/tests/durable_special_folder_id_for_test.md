---
type: Rust Function
title: durable_special_folder_id_for_test
resource: crates/lpe-exchange/src/tests/mod.rs#L15126-L15139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_same_folder_move_is_idempotent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_pending_event_modify_recipients_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_rejects_binary_payload_without_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_rejects_binary_payload_without_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_blob_rejects_without_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_valid_attachments_projects_existing_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_open_attachment_projects_existing_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_open_message_projects_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_ipm_subtree_reports_distinct_folder_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_advertised_special_folder_reports_own_identity
---

# Signature

`async fn durable_special_folder_id_for_test<S>( store: &S, account_id: Uuid, logical_folder_id: u64, ) -> u64 where S: ExchangeStore,`

# Calls

- [load_mapi_identity_codec_for_test](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [actual_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)

# Called by

- [mapi_over_http_calendar_same_folder_move_is_idempotent](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_same_folder_move_is_idempotent.md)
- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_custom_properties_survive_restart_style_session](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session.md)
- [mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields.md)
- [mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder.md)
- [mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition.md)
- [mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle.md)
- [mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards.md)
- [mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction.md)
- [mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream.md)
- [mapi_over_http_calendar_concurrent_rw_handles_require_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save.md)
- [mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid.md)
- [mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids.md)
- [mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids.md)
- [mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids.md)
- [mapi_over_http_empty_advertised_calendar_create_uses_default_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection.md)
- [mapi_over_http_calendar_pending_event_modify_recipients_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_pending_event_modify_recipients_succeeds.md)
- [mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event.md)
- [mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties.md)
- [mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect.md)
- [mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request.md)
- [mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete.md)
- [mapi_over_http_calendar_meeting_cancel_rejects_binary_payload_without_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_rejects_binary_payload_without_side_effect.md)
- [mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status.md)
- [mapi_over_http_calendar_meeting_response_rejects_binary_payload_without_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_rejects_binary_payload_without_side_effect.md)
- [mapi_over_http_calendar_attendee_named_properties_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event.md)
- [mapi_over_http_calendar_display_cc_updates_optional_attendees](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees.md)
- [mapi_over_http_calendar_time_zone_blob_rejects_without_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_blob_rejects_without_side_effect.md)
- [mapi_over_http_calendar_time_zone_description_updates_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event.md)
- [mapi_over_http_calendar_whole_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_common_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status.md)
- [mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_calendar_get_valid_attachments_projects_existing_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_valid_attachments_projects_existing_event.md)
- [mapi_over_http_advertised_calendar_open_attachment_projects_existing_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_open_attachment_projects_existing_event.md)
- [mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons.md)
- [mapi_over_http_advertised_calendar_open_message_projects_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_open_message_projects_default_collection_event.md)
- [mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar.md)
- [mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox.md)
- [mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_ipm_subtree_reports_distinct_folder_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_ipm_subtree_reports_distinct_folder_identity.md)
- [mapi_over_http_advertised_special_folder_reports_own_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_advertised_special_folder_reports_own_identity.md)