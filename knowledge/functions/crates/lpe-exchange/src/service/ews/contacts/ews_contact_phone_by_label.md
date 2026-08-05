---
type: Rust Function
title: ews_contact_phone_by_label
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L377-L379
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_labeled_string
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_entries_xml
---

# Signature

`fn ews_contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> Option<String>`

# Calls

- [contact_labeled_string](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_labeled_string.md)

# Called by

- [ews_contact_phone_entries_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_entries_xml.md)