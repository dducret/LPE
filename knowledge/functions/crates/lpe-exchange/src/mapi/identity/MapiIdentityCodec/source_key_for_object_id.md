---
type: Rust Method
title: source_key_for_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L302-L308
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
---

# Signature

`pub(crate) fn source_key_for_object_id(&self, object_id: u64) -> Option<Vec<u8>>`

# Calls

- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)