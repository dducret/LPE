---
type: Rust Function
title: object_id_from_trailing_replid_wire_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L657-L660
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id
---

# Signature

`pub(crate) fn object_id_from_trailing_replid_wire_id(bytes: &[u8]) -> Option<u64>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_object_id_from_trailing_replid_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id.md)