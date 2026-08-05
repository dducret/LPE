---
type: Rust Function
title: ews_contact_url_by_label
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L381-L384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_labeled_string
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
---

# Signature

`fn ews_contact_url_by_label(contact: &AccessibleContact, labels: &[&str]) -> Option<String>`

# Calls

- [contact_labeled_string](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_labeled_string.md)

# Called by

- [contact_item_xml_with_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)