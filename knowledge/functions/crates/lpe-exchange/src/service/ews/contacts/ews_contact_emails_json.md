---
type: Rust Function
title: ews_contact_emails_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L455-L471
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json
---

# Signature

`fn ews_contact_emails_json(contact: &str, primary: &str) -> serde_json::Value`

# Calls

- [push_json_contact_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)

# Called by

- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [ews_updated_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json.md)