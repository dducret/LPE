---
type: Rust Function
title: write_fixed_query_rows_property_value
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L92-L101
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_fixed_query_rows_property_values
---

# Signature

`fn write_fixed_query_rows_property_value( response: &mut Vec<u8>, values: &[u8], offset: usize, size: usize, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [write_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)
- [write_counted_fixed_query_rows_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_fixed_query_rows_property_values.md)