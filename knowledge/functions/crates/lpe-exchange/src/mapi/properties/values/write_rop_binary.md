---
type: Rust Function
title: write_rop_binary
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L389-L393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_binary
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contents_find_row_matches_message_search_key
---

# Signature

`pub(in crate::mapi) fn write_rop_binary(row: &mut Vec<u8>, value: &[u8])`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_multi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_binary.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [contents_find_row_matches_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contents_find_row_matches_message_search_key.md)