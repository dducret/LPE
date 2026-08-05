---
type: Rust Function
title: raw_object_id_from_folder_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L795-L810
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/identity/is_advertised_special_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_entry_id
---

# Signature

`fn raw_object_id_from_folder_entry_id(entry_id: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [is_advertised_special_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_advertised_special_folder_id.md)

# Called by

- [raw_object_id_from_folder_identifier_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes.md)
- [object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_entry_id.md)