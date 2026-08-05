---
type: Rust Function
title: contact_url_by_label
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L291-L301
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
---

# Signature

`fn contact_url_by_label(contact: &AccessibleContact, labels: &[&str]) -> String`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)