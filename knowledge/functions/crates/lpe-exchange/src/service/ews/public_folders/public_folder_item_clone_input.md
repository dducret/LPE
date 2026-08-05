---
type: Rust Function
title: public_folder_item_clone_input
resource: crates/lpe-exchange/src/service/ews/public_folders.rs#L118-L134
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
---

# Signature

`pub(in crate::service) fn public_folder_item_clone_input( principal: &AccountPrincipal, existing: &PublicFolderItem, target_public_folder_id: Uuid, ) -> UpsertPublicFolderItemInput`

# Called by

- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)