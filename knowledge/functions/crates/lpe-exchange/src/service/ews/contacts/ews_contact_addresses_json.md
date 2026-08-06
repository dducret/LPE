---
type: Rust Function
title: ews_contact_addresses_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L569-L578
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_json
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
---

# Signature

`fn ews_contact_addresses_json(contact: &str) -> serde_json::Value`

# Calls

- [ews_contact_address_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry.md)
- [contact_address_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_json.md)

# Called by

- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)