---
type: Rust Function
title: raw_long_term_id_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L734-L740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/long_term_id_from_object_id
---

# Signature

`fn raw_long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]>`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [long_term_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/long_term_id_from_object_id.md)