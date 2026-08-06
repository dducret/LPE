---
type: Rust Function
title: contact_phones_json_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L584-L600
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/push_labeled_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
---

# Signature

`fn contact_phones_json_from_mapi( existing: &AccessibleContact, primary: &str, mobile: Option<&str>, business: Option<&str>, home: Option<&str>, ) -> serde_json::Value`

# Calls

- [push_labeled_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/push_labeled_value.md)

# Called by

- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)