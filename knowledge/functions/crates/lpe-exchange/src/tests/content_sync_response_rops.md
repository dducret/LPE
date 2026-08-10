---
type: Rust Function
title: content_sync_response_rops
resource: crates/lpe-exchange/src/tests/mod.rs#L15511-L15522
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_baseline_exports_all_current_messages
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
---

# Signature

`async fn content_sync_response_rops( store: FakeStore, folder_global_counter: u64, client_state: &[u8], ) -> Vec<u8>`

# Calls

- [content_sync_response_rops_for_store](../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store.md)
- [test_mapi_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)

# Called by

- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_advertised_calendar_sync_projects_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event.md)
- [mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows.md)
- [mapi_over_http_content_sync_first_baseline_exports_all_current_messages](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_baseline_exports_all_current_messages.md)
- [mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes.md)
- [mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding.md)