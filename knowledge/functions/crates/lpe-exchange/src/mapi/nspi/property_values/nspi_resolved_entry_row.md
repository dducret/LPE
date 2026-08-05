---
type: Rust Function
title: nspi_resolved_entry_row
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L349-L365
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table
---

# Signature

`pub(in crate::mapi) fn nspi_resolved_entry_row( account_id: Uuid, entry: &ExchangeAddressBookEntry, columns: &[u32], directory_entries: &[ExchangeAddressBookEntry], ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)
- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [write_embedded_address_book_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table.md)