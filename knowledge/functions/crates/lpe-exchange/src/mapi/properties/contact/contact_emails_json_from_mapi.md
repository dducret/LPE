---
type: Rust Function
title: contact_emails_json_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L506-L538
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/update_primary_labeled_json
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
---

# Signature

`fn contact_emails_json_from_mapi( existing: &AccessibleContact, primary: &str, email1: Option<&str>, email2: Option<&str>, email3: Option<&str>, ) -> serde_json::Value`

# Calls

- [update_primary_labeled_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/update_primary_labeled_json.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)