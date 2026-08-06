---
type: Rust Function
title: append_search_property_u32
resource: crates/lpe-exchange/src/tests/mod.rs#L15038-L15043
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance
---

# Signature

`fn append_search_property_u32(restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: u32)`

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)
- [mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance.md)