---
type: Rust Function
title: write_query_rows_string8_value
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L103-L122
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_string_values
---

# Signature

`fn write_query_rows_string8_value( response: &mut Vec<u8>, values: &[u8], offset: usize, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)
- [write_counted_query_rows_string_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_counted_query_rows_string_values.md)