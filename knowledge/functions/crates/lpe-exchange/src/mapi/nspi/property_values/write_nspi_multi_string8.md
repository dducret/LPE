---
type: Rust Function
title: write_nspi_multi_string8
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L807-L815
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
---

# Signature

`fn write_nspi_multi_string8(body: &mut Vec<u8>, values: &[String])`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)

# Called by

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)