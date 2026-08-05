---
type: Rust Method
title: per_user_write_data
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L718-L721
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_max_data_size
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn per_user_write_data(&self) -> &[u8]`

# Calls

- [per_user_max_data_size](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_max_data_size.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_write_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)