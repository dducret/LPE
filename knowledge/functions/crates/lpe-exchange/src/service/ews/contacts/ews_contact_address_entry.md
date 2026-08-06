---
type: Rust Function
title: ews_contact_address_entry
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L604-L626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_addresses_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json
---

# Signature

`fn ews_contact_address_entry<'a>(contact: &'a str, key: &str) -> Option<&'a str>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)

# Called by

- [ews_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_addresses_json.md)
- [ews_updated_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json.md)