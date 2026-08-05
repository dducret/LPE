---
type: Rust Function
title: nspi_entry_permanent_entry_id
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1050-L1060
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_record_key
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_required_address_book_properties_match_exchange_identity_contract
  - functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id
---

# Signature

`pub(in crate::mapi) fn nspi_entry_permanent_entry_id(entry: &ExchangeAddressBookEntry) -> Vec<u8>`

# Calls

- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [nspi_entry_display_type](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_entry_record_key](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_record_key.md)
- [nspi_entry_value_with_directory](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)
- [nspi_entry_required_address_book_properties_match_exchange_identity_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_required_address_book_properties_match_exchange_identity_contract.md)
- [sent_representing_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id.md)