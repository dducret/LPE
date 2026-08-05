---
type: Rust Function
title: long_term_id_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L953-L956
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_long_term_id_from_object_id
---

# Signature

`pub(crate) fn long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_long_term_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_long_term_id_from_object_id.md)