---
type: Rust Function
title: object_id_from_long_term_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L958-L961
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id
---

# Signature

`pub(crate) fn object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id.md)