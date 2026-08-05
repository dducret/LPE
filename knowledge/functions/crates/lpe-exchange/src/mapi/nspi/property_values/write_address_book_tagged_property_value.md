---
type: Rust Function
title: write_address_book_tagged_property_value
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L735-L743
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_row
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/address_book_tagged_values_use_mapi_http_layout
---

# Signature

`pub(in crate::mapi) fn write_address_book_tagged_property_value( body: &mut Vec<u8>, property_tag: u32, value: &NspiValue<'_>, )`

# Calls

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)

# Called by

- [nspi_entry_property_value_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list.md)
- [nspi_get_props_property_value_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list.md)
- [nspi_special_table_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_row.md)
- [address_book_tagged_values_use_mapi_http_layout](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/address_book_tagged_values_use_mapi_http_layout.md)