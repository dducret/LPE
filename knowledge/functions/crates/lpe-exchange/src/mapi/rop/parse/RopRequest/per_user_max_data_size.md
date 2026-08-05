---
type: Rust Method
title: per_user_max_data_size
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L706-L712
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_write_data
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn per_user_max_data_size(&self) -> u16`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [per_user_write_data](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_write_data.md)
- [rop_read_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response.md)