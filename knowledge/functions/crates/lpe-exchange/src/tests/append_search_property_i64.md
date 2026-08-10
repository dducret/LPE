---
type: Rust Function
title: append_search_property_i64
resource: crates/lpe-exchange/src/tests/mod.rs#L15222-L15227
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds
---

# Signature

`fn append_search_property_i64(restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: i64)`

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds.md)