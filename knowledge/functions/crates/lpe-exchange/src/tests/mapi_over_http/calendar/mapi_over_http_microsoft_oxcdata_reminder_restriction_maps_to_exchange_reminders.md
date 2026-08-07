---
type: Rust Function
title: mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L9846-L9961
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/append_search_property_binary
  - functions/crates/lpe-exchange/src/tests/append_search_exists
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_search_bitmask
  - functions/crates/lpe-exchange/src/tests/append_search_property_bool
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_set_search_criteria
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders()`

# Calls

- [test_mapi_uuid_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id.md)
- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_search_property_binary](../../../../../../../functions/crates/lpe-exchange/src/tests/append_search_property_binary.md)
- [append_search_exists](../../../../../../../functions/crates/lpe-exchange/src/tests/append_search_exists.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_search_bitmask](../../../../../../../functions/crates/lpe-exchange/src/tests/append_search_bitmask.md)
- [append_search_property_bool](../../../../../../../functions/crates/lpe-exchange/src/tests/append_search_property_bool.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_set_search_criteria](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_search_criteria.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)