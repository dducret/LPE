---
type: Rust Function
title: write_counted_fixed_query_rows_property_values
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L166-L176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_fixed_query_rows_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
---

# Signature

`fn write_counted_fixed_query_rows_property_values( response: &mut Vec<u8>, values: &[u8], offset: usize, value_size: usize, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [write_fixed_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_fixed_query_rows_property_value.md)

# Called by

- [write_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)