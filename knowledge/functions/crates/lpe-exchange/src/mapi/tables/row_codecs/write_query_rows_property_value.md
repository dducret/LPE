---
type: Rust Function
title: write_query_rows_property_value
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L39-L90
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_fixed_query_rows_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_string8_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_utf16_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_binary_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_fixed_query_rows_property_values
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_string_values
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_binary_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row
---

# Signature

`fn write_query_rows_property_value( response: &mut Vec<u8>, property_tag: u32, values: &[u8], offset: usize, ) -> Option<usize>`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [write_fixed_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_fixed_query_rows_property_value.md)
- [write_query_rows_string8_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_string8_value.md)
- [write_query_rows_utf16_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_utf16_value.md)
- [write_query_rows_binary_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_binary_value.md)
- [write_counted_fixed_query_rows_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_fixed_query_rows_property_values.md)
- [write_counted_query_rows_string_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_string_values.md)
- [write_counted_query_rows_binary_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_binary_values.md)

# Called by

- [write_query_rows_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row.md)