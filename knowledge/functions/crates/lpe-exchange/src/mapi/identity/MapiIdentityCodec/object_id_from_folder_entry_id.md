---
type: Rust Method
title: object_id_from_folder_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L379-L391
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
---

# Signature

`pub(crate) fn object_id_from_folder_entry_id(&self, entry_id: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)