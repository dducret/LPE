---
type: Rust Function
title: content_sync_response_rops_for_store
resource: crates/lpe-exchange/src/tests/mod.rs#L15524-L15533
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops
---

# Signature

`async fn content_sync_response_rops_for_store<S>( store: S, folder_id: u64, client_state: &[u8], ) -> Vec<u8> where S: ExchangeStore + Clone + Send + Sync + 'static,`

# Calls

- [content_sync_response_rops_for_store_with_flags](../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags.md)

# Called by

- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties.md)
- [mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection.md)
- [mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar.md)
- [mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection.md)
- [mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_contacts_search_content_sync_uses_search_folder_parent](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent.md)
- [mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets.md)
- [content_sync_response_rops](../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops.md)