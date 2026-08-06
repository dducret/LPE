---
type: Rust Function
title: ews_contact_phone_entries_xml
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L357-L384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_by_label
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
---

# Signature

`fn ews_contact_phone_entries_xml(contact: &AccessibleContact) -> String`

# Calls

- [ews_contact_phone_by_label](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_by_label.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_item_xml_with_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)