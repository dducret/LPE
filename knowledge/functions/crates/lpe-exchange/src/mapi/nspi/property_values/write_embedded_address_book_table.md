---
type: Rust Function
title: write_embedded_address_book_table
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L825-L838
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
---

# Signature

`fn write_embedded_address_book_table( body: &mut Vec<u8>, account_id: Uuid, entries: &[ExchangeAddressBookEntry], )`

# Calls

- [write_large_property_tag_array](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array.md)
- [nspi_resolved_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row.md)

# Called by

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)