---
type: Rust Method
title: per_user_has_finished
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L714-L716
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn per_user_has_finished(&self) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_write_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)