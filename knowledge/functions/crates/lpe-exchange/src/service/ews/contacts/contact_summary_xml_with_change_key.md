---
type: Rust Function
title: contact_summary_xml_with_change_key
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L7-L23
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
---

# Signature

`pub(in crate::service) fn contact_summary_xml_with_change_key( contact: &AccessibleContact, change_key: &str, ) -> String`

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)