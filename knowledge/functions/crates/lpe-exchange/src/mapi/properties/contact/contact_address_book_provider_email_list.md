---
type: Rust Function
title: contact_address_book_provider_email_list
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L283-L289
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
---

# Signature

`fn contact_address_book_provider_email_list(contact: &AccessibleContact) -> Option<Vec<i32>>`

# Calls

- [contact_email_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value.md)

# Called by

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)