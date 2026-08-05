---
type: Rust Function
title: append_search_property_binary
resource: crates/lpe-exchange/src/tests/mod.rs#L14977-L14986
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id
---

# Signature

`fn append_search_property_binary( restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: &[u8], )`

# Calls

- [append_mapi_binary_property](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)

# Called by

- [mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id.md)
- [mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox.md)
- [mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox.md)
- [mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders.md)
- [mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id.md)