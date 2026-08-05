---
type: Rust Method
title: collapse_state_row_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1201-L1209
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response
---

# Signature

`pub(in crate::mapi) fn collapse_state_row_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_get_collapse_state_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response.md)