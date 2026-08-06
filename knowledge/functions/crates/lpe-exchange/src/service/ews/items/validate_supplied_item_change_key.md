---
type: Rust Function
title: validate_supplied_item_change_key
resource: crates/lpe-exchange/src/service/ews/items.rs#L1539-L1552
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/items/stale_supplied_change_key_is_rejected_before_item_mutation
---

# Signature

`fn validate_supplied_item_change_key( references: &[RequestedItemReference], id: &str, current_change_key: &str, ) -> Result<()>`

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [validate_mutating_item_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [stale_supplied_change_key_is_rejected_before_item_mutation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/stale_supplied_change_key_is_rejected_before_item_mutation.md)