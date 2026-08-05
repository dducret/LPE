---
type: Rust Function
title: deleted_or_updated_contact_entry
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L436-L453
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
---

# Signature

`fn deleted_or_updated_contact_entry( request: &str, contact: &str, field_uris: &[&str], collection_name: &str, keys: &[&str], existing: &str, ) -> String`

# Calls

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)

# Called by

- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)