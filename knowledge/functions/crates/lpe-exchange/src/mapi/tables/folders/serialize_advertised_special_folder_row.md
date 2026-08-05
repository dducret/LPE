---
type: Rust Function
title: serialize_advertised_special_folder_row
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L288-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row
---

# Signature

`fn serialize_advertised_special_folder_row( folder_id: u64, columns: &[u32], principal: Option<&AccountPrincipal>, ) -> Vec<u8>`

# Calls

- [serialize_advertised_special_folder_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid.md)

# Called by

- [serialize_special_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row.md)