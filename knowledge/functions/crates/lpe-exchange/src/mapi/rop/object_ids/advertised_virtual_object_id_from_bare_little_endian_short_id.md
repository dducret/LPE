---
type: Rust Function
title: advertised_virtual_object_id_from_bare_little_endian_short_id
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L102-L110
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/global_counter_from_little_endian_globcnt
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id
---

# Signature

`fn advertised_virtual_object_id_from_bare_little_endian_short_id(bytes: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_little_endian_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/global_counter_from_little_endian_globcnt.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)

# Called by

- [stale_special_folder_object_id_from_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)