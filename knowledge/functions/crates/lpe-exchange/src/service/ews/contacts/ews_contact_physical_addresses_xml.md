---
type: Rust Function
title: ews_contact_physical_addresses_xml
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L386-L416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_by_label
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
---

# Signature

`fn ews_contact_physical_addresses_xml(contact: &AccessibleContact) -> String`

# Calls

- [contact_address_by_label](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_by_label.md)

# Called by

- [contact_item_xml_with_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)