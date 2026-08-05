---
type: Rust Method
title: fractional_position
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1130-L1134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_fractional_request_is_valid
---

# Signature

`pub(in crate::mapi) fn fractional_position(&self) -> Option<(u32, u32)>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_row_fractional_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response.md)
- [seek_row_fractional_request_is_valid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_fractional_request_is_valid.md)