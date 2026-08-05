---
type: Rust Function
title: public_folder_item_summary_xml
resource: crates/lpe-exchange/src/service/ews/public_folders.rs#L11-L34
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn public_folder_item_summary_xml(item: &PublicFolderItem) -> String`

# Called by

- [public_folder_item_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)