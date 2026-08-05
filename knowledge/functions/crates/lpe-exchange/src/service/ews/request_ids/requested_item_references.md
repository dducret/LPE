---
type: Rust Function
title: requested_item_references
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L16-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/item_references_keep_supplied_change_keys_with_their_item_ids
---

# Signature

`pub(in crate::service) fn requested_item_references(request: &str) -> Vec<RequestedItemReference>`

# Calls

- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [validate_mutating_item_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [requested_item_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [item_references_keep_supplied_change_keys_with_their_item_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/item_references_keep_supplied_change_keys_with_their_item_ids.md)