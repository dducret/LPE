---
type: Rust Method
title: object_id_from_trailing_replid_wire_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L292-L296
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/is_advertised_special_folder_id
---

# Signature

`pub(crate) fn object_id_from_trailing_replid_wire_id(&self, bytes: &[u8]) -> Option<u64>`

# Calls

- [raw_object_id_from_trailing_replid_wire_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)
- [is_advertised_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_advertised_special_folder_id.md)