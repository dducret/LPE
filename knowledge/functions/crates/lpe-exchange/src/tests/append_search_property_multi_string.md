---
type: Rust Function
title: append_search_property_multi_string
resource: crates/lpe-exchange/src/tests/mod.rs#L15116-L15129
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_search_folder_persists_only_after_criteria
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_root_search_folder_accepts_criteria
---

# Signature

`fn append_search_property_multi_string( restriction: &mut Vec<u8>, property_tag: u32, relop: u8, values: &[&str], )`

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)
- [mapi_over_http_create_search_folder_persists_only_after_criteria](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_search_folder_persists_only_after_criteria.md)
- [mapi_over_http_create_root_search_folder_accepts_criteria](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_root_search_folder_accepts_criteria.md)