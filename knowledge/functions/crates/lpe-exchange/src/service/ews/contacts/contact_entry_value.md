---
type: Rust Function
title: contact_entry_value
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L406-L434
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/xml/xml_text
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json
---

# Signature

`fn contact_entry_value(contact: &str, collection_name: &str, key: &str) -> Option<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [xml_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/xml_text.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [deleted_or_updated_contact_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry.md)
- [ews_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json.md)
- [ews_contact_phones_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json.md)