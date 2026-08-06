---
type: Rust Function
title: upsert_labeled_value
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L624-L647
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_urls_json_from_mapi
---

# Signature

`fn upsert_labeled_value( rows: &mut Vec<serde_json::Value>, key: &str, label: &str, value: Option<&str>, )`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_urls_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_urls_json_from_mapi.md)