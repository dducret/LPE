---
type: Rust Function
title: append_mapi_trailing_replid_wire_id
resource: crates/lpe-exchange/src/tests/mod.rs#L14823-L14826
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_long_term_id_round_trips_canonical_replica_ids
---

# Signature

`fn append_mapi_trailing_replid_wire_id(buffer: &mut Vec<u8>, global_counter: u64)`

# Called by

- [mapi_over_http_long_term_id_round_trips_canonical_replica_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_long_term_id_round_trips_canonical_replica_ids.md)