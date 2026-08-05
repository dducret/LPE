---
type: Rust Function
title: requested_public_folder_ids
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L102-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/move_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/delete_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/mark_all_items_as_read
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn requested_public_folder_ids(request: &str) -> Vec<Uuid>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [create_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder.md)
- [copy_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)
- [move_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/move_folder.md)
- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [delete_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/delete_folder.md)
- [get_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)
- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [mark_all_items_as_read](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/mark_all_items_as_read.md)
- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)