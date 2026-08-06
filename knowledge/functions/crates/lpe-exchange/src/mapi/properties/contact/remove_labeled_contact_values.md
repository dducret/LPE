---
type: Rust Function
title: remove_labeled_contact_values
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L779-L800
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions
---

# Signature

`fn remove_labeled_contact_values( value: serde_json::Value, key: &str, labels: &[&str], ) -> serde_json::Value`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [contact_input_from_mapi_with_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions.md)