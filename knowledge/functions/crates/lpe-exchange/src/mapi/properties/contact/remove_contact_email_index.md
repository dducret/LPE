---
type: Rust Function
title: remove_contact_email_index
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L802-L812
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions
---

# Signature

`fn remove_contact_email_index(value: serde_json::Value, index: usize) -> serde_json::Value`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [contact_input_from_mapi_with_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions.md)