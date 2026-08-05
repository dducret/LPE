---
type: Rust Function
title: write_address_book_property_value
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L745-L805
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string8
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_binary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value
---

# Signature

`pub(in crate::mapi) fn write_address_book_property_value( body: &mut Vec<u8>, property_tag: u32, value: &NspiValue<'_>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_nspi_multi_string8](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string8.md)
- [write_nspi_multi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string.md)
- [write_embedded_address_book_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_embedded_address_book_table.md)
- [write_nspi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_binary.md)

# Called by

- [nspi_resolved_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row.md)
- [write_address_book_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value.md)