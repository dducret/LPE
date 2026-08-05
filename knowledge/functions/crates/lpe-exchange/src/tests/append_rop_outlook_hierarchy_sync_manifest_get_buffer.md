---
type: Rust Function
title: append_rop_outlook_hierarchy_sync_manifest_get_buffer
resource: crates/lpe-exchange/src/tests/mod.rs#L15338-L15352
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_hierarchy_sync_does_not_publish_recoverable_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_move_folder_updates_custom_canonical_mailbox_and_hierarchy_sync
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_real_conversation_history_mailbox_stays_out_of_startup_hierarchy_sync
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_default_folder_probe_after_hierarchy_sync_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_root_hierarchy_sync_keeps_parent_keys_root_relative
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_fast_transfer_stream_decodes_strictly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
---

# Signature

`fn append_rop_outlook_hierarchy_sync_manifest_get_buffer( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, )`

# Calls

- [append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state.md)

# Called by

- [mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_hierarchy_sync_does_not_publish_recoverable_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_hierarchy_sync_does_not_publish_recoverable_items.md)
- [mapi_over_http_move_folder_updates_custom_canonical_mailbox_and_hierarchy_sync](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_move_folder_updates_custom_canonical_mailbox_and_hierarchy_sync.md)
- [mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders.md)
- [mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders.md)
- [mapi_over_http_real_conversation_history_mailbox_stays_out_of_startup_hierarchy_sync](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_real_conversation_history_mailbox_stays_out_of_startup_hierarchy_sync.md)
- [mapi_over_http_default_folder_probe_after_hierarchy_sync_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_default_folder_probe_after_hierarchy_sync_succeeds.md)
- [mapi_over_http_root_hierarchy_sync_keeps_parent_keys_root_relative](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_root_hierarchy_sync_keeps_parent_keys_root_relative.md)
- [mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys.md)
- [mapi_over_http_hierarchy_sync_fast_transfer_stream_decodes_strictly](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_fast_transfer_stream_decodes_strictly.md)
- [mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)