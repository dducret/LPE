---
type: Rust Function
title: append_search_exists
resource: crates/lpe-exchange/src/tests/mod.rs#L14988-L14991
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction
---

# Signature

`fn append_search_exists(restriction: &mut Vec<u8>, property_tag: u32)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders.md)
- [mapi_over_http_set_get_search_criteria_round_trips_attachment_exists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists.md)
- [mapi_over_http_set_search_criteria_rejects_unsupported_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction.md)