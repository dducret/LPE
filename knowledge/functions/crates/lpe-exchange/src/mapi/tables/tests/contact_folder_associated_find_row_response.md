---
type: Rust Function
title: contact_folder_associated_find_row_response
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8569-L8614
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_contact_folder_associated_find_row_does_not_return_config
---

# Signature

`fn contact_folder_associated_find_row_response( folder_id: u64, message_class: &str, snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)

# Called by

- [assert_contact_folder_associated_find_row_does_not_return_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_contact_folder_associated_find_row_does_not_return_config.md)