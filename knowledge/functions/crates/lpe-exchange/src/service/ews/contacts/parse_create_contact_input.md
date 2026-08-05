---
type: Rust Function
title: parse_create_contact_input
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L121-L180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn parse_create_contact_input( principal: &AccountPrincipal, request: &str, ) -> Result<UpsertClientContactInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [ews_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_emails_json.md)
- [ews_contact_phones_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json.md)
- [ews_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_urls_json.md)

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)