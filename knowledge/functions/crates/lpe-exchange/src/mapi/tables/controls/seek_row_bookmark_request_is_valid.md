---
type: Rust Function
title: seek_row_bookmark_request_is_valid
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L242-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
---

# Signature

`fn seek_row_bookmark_request_is_valid(request: &RopRequest) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)