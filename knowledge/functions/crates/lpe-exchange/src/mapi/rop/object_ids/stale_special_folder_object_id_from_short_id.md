---
type: Rust Function
title: stale_special_folder_object_id_from_short_id
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L68-L100
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/global_counter_from_little_endian_globcnt
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/dynamic_object_id_from_bare_little_endian_short_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id
---

# Signature

`fn stale_special_folder_object_id_from_short_id(bytes: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [global_counter_from_little_endian_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/global_counter_from_little_endian_globcnt.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [advertised_virtual_object_id_from_bare_little_endian_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id.md)
- [dynamic_object_id_from_bare_little_endian_short_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/dynamic_object_id_from_bare_little_endian_short_id.md)

# Called by

- [long_term_source_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id.md)