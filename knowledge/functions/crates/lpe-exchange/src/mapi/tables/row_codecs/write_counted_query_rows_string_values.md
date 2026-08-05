---
type: Rust Function
title: write_counted_query_rows_string_values
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L178-L196
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_utf16_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_string8_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
---

# Signature

`fn write_counted_query_rows_string_values( response: &mut Vec<u8>, values: &[u8], offset: usize, unicode: bool, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [write_query_rows_utf16_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_utf16_value.md)
- [write_query_rows_string8_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_string8_value.md)

# Called by

- [write_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)