---
type: Rust Function
title: contact_email_value
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L281-L296
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_json_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_address_book_provider_email_list
---

# Signature

`fn contact_email_value(contact: &AccessibleContact, index: usize) -> Option<String>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [contact_json_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_json_values.md)

# Called by

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [contact_address_book_provider_email_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_address_book_provider_email_list.md)