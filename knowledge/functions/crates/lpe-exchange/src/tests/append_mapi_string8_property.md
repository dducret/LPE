---
type: Rust Function
title: append_mapi_string8_property
resource: crates/lpe-exchange/src/tests/mod.rs#L15055-L15059
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property
  - functions/crates/lpe-exchange/src/tests/append_search_content_string8
---

# Signature

`fn append_mapi_string8_property(values: &mut Vec<u8>, property_tag: u32, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property.md)
- [append_search_content_string8](../../../../../functions/crates/lpe-exchange/src/tests/append_search_content_string8.md)