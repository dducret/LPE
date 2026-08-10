---
type: Rust Function
title: read_u64_from
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L292-L296
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response
---

# Signature

`fn read_u64_from(bytes: &[u8], offset: &mut usize) -> Option<u64>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response.md)