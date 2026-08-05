---
type: Rust Method
title: per_user_data_offset
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L698-L704
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn per_user_data_offset(&self) -> u32`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_write_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)
- [rop_read_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response.md)