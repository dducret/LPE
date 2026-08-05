---
type: Rust Function
title: query_rows_truncates_variable_property_values_to_microsoft_limit
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L1350-L1374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
---

# Signature

`fn query_rows_truncates_variable_property_values_to_microsoft_limit()`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_query_rows_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)