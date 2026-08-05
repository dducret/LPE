---
type: Rust Function
title: calendar_item_summary_xml_with_change_key
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L8-L27
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
---

# Signature

`pub(in crate::service) fn calendar_item_summary_xml_with_change_key( event: &AccessibleEvent, change_key: &str, ) -> String`

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)