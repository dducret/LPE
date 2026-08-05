---
type: Rust Function
title: append_rop_set_search_criteria
resource: crates/lpe-exchange/src/tests/mod.rs#L14921-L14936
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_search_folder_persists_only_after_criteria
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_root_search_folder_accepts_criteria
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags
---

# Signature

`fn append_rop_set_search_criteria( rops: &mut Vec<u8>, input: u8, restriction: &[u8], folder_ids: &[u64], flags: u32, )`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)

# Called by

- [mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh.md)
- [mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders.md)
- [mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds.md)
- [mapi_over_http_set_get_search_criteria_round_trips_attachment_exists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists.md)
- [mapi_over_http_set_get_search_criteria_round_trips_read_bitmask](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask.md)
- [mapi_over_http_set_get_search_criteria_round_trips_string8_body_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content.md)
- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)
- [mapi_over_http_create_search_folder_persists_only_after_criteria](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_search_folder_persists_only_after_criteria.md)
- [mapi_over_http_create_root_search_folder_accepts_criteria](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_root_search_folder_accepts_criteria.md)
- [mapi_over_http_set_get_search_criteria_updates_canonical_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder.md)
- [mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance.md)
- [mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction.md)
- [mapi_over_http_set_search_criteria_rejects_unsupported_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags.md)