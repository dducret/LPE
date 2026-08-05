---
type: Rust Function
title: write_nspi_binary
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L840-L846
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
---

# Signature

`fn write_nspi_binary(body: &mut Vec<u8>, value: &[u8])`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)