---
type: Rust Function
title: contact_labeled_string
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L386-L404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_by_label
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_url_by_label
---

# Signature

`fn contact_labeled_string(value: &serde_json::Value, key: &str, labels: &[&str]) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [ews_contact_phone_by_label](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_by_label.md)
- [ews_contact_url_by_label](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_url_by_label.md)