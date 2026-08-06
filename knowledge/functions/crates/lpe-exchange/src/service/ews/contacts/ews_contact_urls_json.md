---
type: Rust Function
title: ews_contact_urls_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L552-L567
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json
---

# Signature

`fn ews_contact_urls_json(contact: &str) -> serde_json::Value`

# Calls

- [push_json_contact_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/push_json_contact_value.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [ews_updated_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json.md)