---
type: Rust Function
title: create_event_success_response
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L61-L91
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn create_event_success_response( event: &AccessibleEvent, change_key: &str, ) -> String`

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)