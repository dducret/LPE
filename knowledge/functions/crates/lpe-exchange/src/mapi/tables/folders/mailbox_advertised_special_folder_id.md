---
type: Rust Function
title: mailbox_advertised_special_folder_id
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L36-L41
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
---

# Signature

`fn mailbox_advertised_special_folder_id(mailbox: &JmapMailbox) -> Option<u64>`

# Calls

- [mapi_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id.md)
- [advertised_special_folder_id_for_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create.md)

# Called by

- [folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)