---
type: Rust Function
title: raw_instance_key_for_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L949-L951
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/instance_key_for_object_id
---

# Signature

`fn raw_instance_key_for_object_id(object_id: u64) -> Vec<u8>`

# Calls

- [raw_source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id.md)

# Called by

- [instance_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/instance_key_for_object_id.md)