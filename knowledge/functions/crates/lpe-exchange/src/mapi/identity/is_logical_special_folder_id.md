---
type: Rust Function
title: is_logical_special_folder_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L564-L566
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
---

# Signature

`pub(crate) fn is_logical_special_folder_id(object_id: u64) -> bool`

# Calls

- [logical_special_folder_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids.md)

# Called by

- [actual_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [logical_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)