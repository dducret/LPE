---
type: Rust Function
title: ews_contact_email_entries_xml
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L316-L346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
---

# Signature

`fn ews_contact_email_entries_xml(contact: &AccessibleContact) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_item_xml_with_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)