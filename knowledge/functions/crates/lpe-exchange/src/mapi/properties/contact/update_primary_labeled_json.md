---
type: Rust Function
title: update_primary_labeled_json
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L539-L566
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_emails_json_from_mapi
---

# Signature

`fn update_primary_labeled_json( existing: &serde_json::Value, key: &str, label: &str, value: &str, ) -> serde_json::Value`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_emails_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_emails_json_from_mapi.md)