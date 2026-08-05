---
type: Rust Function
title: contact_json_values
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L328-L338
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value
---

# Signature

`fn contact_json_values(value: &serde_json::Value, key: &str) -> Vec<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [contact_email_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value.md)