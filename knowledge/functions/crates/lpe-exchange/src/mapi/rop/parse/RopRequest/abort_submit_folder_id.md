---
type: Rust Method
title: abort_submit_folder_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L94-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response
---

# Signature

`pub(in crate::mapi) fn abort_submit_folder_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_abort_submit_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)