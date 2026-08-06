---
type: Rust Function
title: create_contact_success_response
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L95-L122
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn create_contact_success_response( contact: &AccessibleContact, change_key: &str, ) -> String`

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)