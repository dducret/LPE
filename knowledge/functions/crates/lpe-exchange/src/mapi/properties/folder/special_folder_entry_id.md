---
type: Rust Function
title: special_folder_entry_id
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L246-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id_value
  - functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids_ex
---

# Signature

`fn special_folder_entry_id(mailbox_guid: Uuid, folder_id: u64) -> Vec<u8>`

# Calls

- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [special_folder_entry_id_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id_value.md)
- [additional_ren_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids.md)
- [additional_ren_entry_ids_ex](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/additional_ren_entry_ids_ex.md)