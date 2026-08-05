---
type: Rust Function
title: append_search_property_string
resource: crates/lpe-exchange/src/tests/mod.rs#L14923-L14932
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
---

# Signature

`fn append_search_property_string( restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: &str, )`

# Calls

- [append_mapi_utf16_property](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)