---
type: Rust Function
title: contact_item_xml_with_change_key
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L25-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_email_entries_xml
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_entries_xml
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_physical_addresses_xml
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_url_by_label
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn contact_item_xml_with_change_key( contact: &AccessibleContact, change_key: &str, ) -> String`

# Calls

- [ews_contact_email_entries_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_email_entries_xml.md)
- [ews_contact_phone_entries_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phone_entries_xml.md)
- [ews_contact_physical_addresses_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_physical_addresses_xml.md)
- [ews_contact_url_by_label](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_url_by_label.md)

# Called by

- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)