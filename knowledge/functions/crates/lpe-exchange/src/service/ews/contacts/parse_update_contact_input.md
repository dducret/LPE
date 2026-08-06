---
type: Rust Function
title: parse_update_contact_input
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L188-L323
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_phones_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_narrow_update_omits_unowned_rich_fields
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`pub(in crate::service) fn parse_update_contact_input( principal: &AccountPrincipal, existing: &AccessibleContact, request: &str, ) -> UpsertClientContactInput`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [deleted_or_updated_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [deleted_or_updated_contact_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry.md)
- [ews_updated_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json.md)
- [ews_updated_contact_phones_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_phones_json.md)
- [ews_updated_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json.md)
- [ews_updated_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json.md)

# Called by

- [ews_contact_narrow_update_omits_unowned_rich_fields](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_narrow_update_omits_unowned_rich_fields.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)