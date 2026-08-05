---
type: Rust Function
title: append_search_bitmask
resource: crates/lpe-exchange/src/tests/mod.rs#L14911-L14921
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction
---

# Signature

`fn append_search_bitmask( restriction: &mut Vec<u8>, property_tag: u32, must_be_nonzero: bool, mask: u32, )`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders.md)
- [mapi_over_http_set_get_search_criteria_round_trips_read_bitmask](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask.md)
- [mapi_over_http_set_search_criteria_rejects_unsupported_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction.md)