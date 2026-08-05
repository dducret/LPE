---
type: Rust Function
title: raw_object_id_from_source_key
resource: crates/lpe-exchange/src/mapi/identity.rs#L932-L941
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_source_key
---

# Signature

`fn raw_object_id_from_source_key(source_key: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [object_id_from_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_source_key.md)