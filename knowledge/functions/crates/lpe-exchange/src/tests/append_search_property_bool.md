---
type: Rust Function
title: append_search_property_bool
resource: crates/lpe-exchange/src/tests/mod.rs#L14958-L14968
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction
---

# Signature

`fn append_search_property_bool( restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: bool, )`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh.md)
- [mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders.md)
- [mapi_over_http_set_get_search_criteria_updates_canonical_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder.md)
- [mapi_over_http_set_search_criteria_rejects_unsupported_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction.md)