---
type: Rust Function
title: public_folder_item_xml
resource: crates/lpe-exchange/src/service/ews/public_folders.rs#L36-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_summary_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
---

# Signature

`pub(in crate::service) fn public_folder_item_xml(item: &PublicFolderItem) -> String`

# Calls

- [public_folder_item_summary_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_summary_xml.md)

# Called by

- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)