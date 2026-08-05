---
type: Rust Function
title: global_counter_from_little_endian_globcnt
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L120-L126
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/dynamic_object_id_from_bare_little_endian_short_id
---

# Signature

`fn global_counter_from_little_endian_globcnt(bytes: &[u8]) -> Option<u64>`

# Called by

- [stale_special_folder_object_id_from_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)
- [advertised_virtual_object_id_from_bare_little_endian_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id.md)
- [dynamic_object_id_from_bare_little_endian_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/dynamic_object_id_from_bare_little_endian_short_id.md)