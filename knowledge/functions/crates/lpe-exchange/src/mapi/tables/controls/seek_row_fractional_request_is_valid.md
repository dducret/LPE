---
type: Rust Function
title: seek_row_fractional_request_is_valid
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L289-L293
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fractional_position
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response
---

# Signature

`fn seek_row_fractional_request_is_valid(request: &RopRequest) -> bool`

# Calls

- [fractional_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fractional_position.md)

# Called by

- [rop_seek_row_fractional_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response.md)