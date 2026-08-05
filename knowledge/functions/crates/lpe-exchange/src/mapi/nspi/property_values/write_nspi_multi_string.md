---
type: Rust Function
title: write_nspi_multi_string
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L817-L823
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
---

# Signature

`fn write_nspi_multi_string(body: &mut Vec<u8>, values: &[String])`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)

# Called by

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)