---
type: Rust Function
title: mailbox_folder_xml
resource: crates/lpe-exchange/src/service/ews/folders.rs#L837-L875
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/folder_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/move_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`pub(in crate::service) fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String`

# Calls

- [folder_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folder_change_key.md)

# Called by

- [create_folder_path](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)
- [copy_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)
- [move_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/move_folder.md)
- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [find_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder.md)
- [sync_folder_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy.md)
- [create_managed_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder.md)
- [get_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)