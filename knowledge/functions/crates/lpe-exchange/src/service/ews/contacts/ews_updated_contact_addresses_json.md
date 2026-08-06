---
type: Rust Function
title: ews_updated_contact_addresses_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L580-L602
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/contacts/remove_contact_address
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_json
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/physical_addresses_round_trip_and_targeted_update_preserves_other_rows
---

# Signature

`fn ews_updated_contact_addresses_json( request: &str, contact: &str, existing: &AccessibleContact, ) -> serde_json::Value`

# Calls

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [remove_contact_address](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/remove_contact_address.md)
- [ews_contact_address_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry.md)
- [contact_address_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_address_json.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [physical_addresses_round_trip_and_targeted_update_preserves_other_rows](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/physical_addresses_round_trip_and_targeted_update_preserves_other_rows.md)