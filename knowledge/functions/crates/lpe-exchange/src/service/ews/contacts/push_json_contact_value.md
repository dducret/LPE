---
type: Rust Function
title: push_json_contact_value
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L553-L562
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/update_first_json_contact_value
---

# Signature

`fn push_json_contact_value( rows: &mut Vec<serde_json::Value>, key: &str, label: &str, value: Option<&str>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [ews_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json.md)
- [ews_contact_phones_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json.md)
- [ews_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json.md)
- [update_first_json_contact_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/update_first_json_contact_value.md)