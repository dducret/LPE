---
type: Rust Method
title: wire_id_bytes_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L298-L300
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
---

# Signature

`pub(crate) fn wire_id_bytes_from_object_id(&self, object_id: u64) -> Option<[u8; 8]>`

# Calls

- [raw_wire_id_bytes_from_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id.md)
- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)