---
type: Rust Function
title: append_search_content_string8
resource: crates/lpe-exchange/src/tests/mod.rs#L15195-L15201
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/append_mapi_string8_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content
---

# Signature

`fn append_search_content_string8(restriction: &mut Vec<u8>, property_tag: u32, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_mapi_string8_property](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_string8_property.md)

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_string8_body_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content.md)