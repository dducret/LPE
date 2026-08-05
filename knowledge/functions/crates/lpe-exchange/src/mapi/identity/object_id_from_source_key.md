---
type: Rust Function
title: object_id_from_source_key
resource: crates/lpe-exchange/src/mapi/identity.rs#L1029-L1032
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_source_key
---

# Signature

`pub(crate) fn object_id_from_source_key(source_key: &[u8]) -> Option<u64>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_object_id_from_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_source_key.md)