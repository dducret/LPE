---
type: Rust Function
title: validate_required_item_change_key
resource: crates/lpe-exchange/src/service/ews/items.rs#L1590-L1604
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
  - functions/crates/lpe-exchange/src/service/ews/items/missing_required_change_key_is_a_conflict
---

# Signature

`fn validate_required_item_change_key( references: &[RequestedItemReference], id: &str, current_change_key: &str, ) -> Result<()>`

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [delete_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [missing_required_change_key_is_a_conflict](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/missing_required_change_key_is_a_conflict.md)