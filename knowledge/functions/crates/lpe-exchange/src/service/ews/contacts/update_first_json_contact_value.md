---
type: Rust Function
title: update_first_json_contact_value
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L712-L729
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json
---

# Signature

`fn update_first_json_contact_value( existing: &serde_json::Value, key: &str, value: &str, ) -> serde_json::Value`

# Calls

- [push_json_contact_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value.md)

# Called by

- [ews_updated_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json.md)