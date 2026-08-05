---
type: Rust Method
title: expand_max_row_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1181-L1188
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response
---

# Signature

`pub(in crate::mapi) fn expand_max_row_count(&self) -> usize`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_expand_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response.md)