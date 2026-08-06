---
type: Rust Function
title: ews_updated_contact_emails_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L657-L671
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/update_first_json_contact_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
---

# Signature

`fn ews_updated_contact_emails_json( request: &str, contact: &str, existing: &AccessibleContact, primary: &str, ) -> serde_json::Value`

# Calls

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [ews_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json.md)
- [update_first_json_contact_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/update_first_json_contact_value.md)

# Called by

- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)