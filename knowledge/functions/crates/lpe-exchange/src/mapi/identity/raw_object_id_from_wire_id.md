---
type: Rust Function
title: raw_object_id_from_wire_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L622-L631
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_wire_id
---

# Signature

`fn raw_object_id_from_wire_id(bytes: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [object_id_from_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id.md)
- [object_id_from_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_wire_id.md)