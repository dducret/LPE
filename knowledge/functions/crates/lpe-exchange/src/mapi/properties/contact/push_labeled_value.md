---
type: Rust Function
title: push_labeled_value
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L613-L622
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_phones_json_from_mapi
---

# Signature

`fn push_labeled_value( rows: &mut Vec<serde_json::Value>, key: &str, label: &str, value: Option<&str>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_phones_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_phones_json_from_mapi.md)