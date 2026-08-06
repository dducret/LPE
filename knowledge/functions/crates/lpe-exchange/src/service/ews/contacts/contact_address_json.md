---
type: Rust Function
title: contact_address_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L628-L646
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_addresses_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json
---

# Signature

`fn contact_address_json(label: &str, entry: &str) -> Option<serde_json::Value>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [ews_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_addresses_json.md)
- [ews_updated_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json.md)