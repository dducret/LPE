---
type: Rust Function
title: nspi_entry_search_key
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1065-L1070
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
---

# Signature

`fn nspi_entry_search_key(entry: &ExchangeAddressBookEntry) -> Vec<u8>`

# Calls

- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_entry_value_with_directory](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)