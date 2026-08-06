---
type: Rust Function
title: update_item_change_content
resource: crates/lpe-exchange/src/service/ews/items.rs#L1579-L1588
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`fn update_item_change_content<'a>( changes: &'a [UpdateItemChange<'a>], id: &str, ) -> Result<&'a str>`

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)