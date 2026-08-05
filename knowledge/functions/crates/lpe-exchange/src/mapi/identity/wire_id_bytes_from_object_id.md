---
type: Rust Function
title: wire_id_bytes_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L662-L665
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id
---

# Signature

`pub(crate) fn wire_id_bytes_from_object_id(object_id: u64) -> Option<[u8; 8]>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_wire_id_bytes_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id.md)