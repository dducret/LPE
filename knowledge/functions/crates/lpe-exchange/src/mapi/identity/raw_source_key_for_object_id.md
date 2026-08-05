---
type: Rust Function
title: raw_source_key_for_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L916-L922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_instance_key_for_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/source_key_for_object_id
---

# Signature

`fn raw_source_key_for_object_id(object_id: u64) -> Vec<u8>`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [raw_instance_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_instance_key_for_object_id.md)
- [source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_key_for_object_id.md)