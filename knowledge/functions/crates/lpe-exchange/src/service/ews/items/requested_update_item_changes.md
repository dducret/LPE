---
type: Rust Function
title: requested_update_item_changes
resource: crates/lpe-exchange/src/service/ews/items.rs#L1559-L1577
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/update_item_changes_keep_each_item_payload_local
---

# Signature

`fn requested_update_item_changes(request: &str) -> Result<Vec<UpdateItemChange<'_>>>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [requested_item_references](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [update_item_changes_keep_each_item_payload_local](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/update_item_changes_keep_each_item_payload_local.md)