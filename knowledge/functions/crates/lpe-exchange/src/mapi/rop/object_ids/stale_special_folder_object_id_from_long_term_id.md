---
type: Rust Function
title: stale_special_folder_object_id_from_long_term_id
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L59-L66
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
---

# Signature

`fn stale_special_folder_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)