---
type: Rust Function
title: calendar_item_xml_with_change_key
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L29-L59
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn calendar_item_xml_with_change_key( event: &AccessibleEvent, change_key: &str, ) -> String`

# Called by

- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)