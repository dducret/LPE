---
type: Rust Function
title: ews_updated_contact_urls_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L685-L699
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
---

# Signature

`fn ews_updated_contact_urls_json( request: &str, contact: &str, existing: &AccessibleContact, ) -> serde_json::Value`

# Calls

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [ews_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json.md)

# Called by

- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)