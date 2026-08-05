---
type: Rust Function
title: rop_expand_row_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L329-L345
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response
---

# Signature

`pub(in crate::mapi) fn rop_expand_row_success_response( request: &RopRequest, expanded_row_count: usize, rows: Vec<Vec<u8>>, ) -> Vec<u8>`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [write_standard_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row.md)

# Called by

- [rop_expand_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response.md)