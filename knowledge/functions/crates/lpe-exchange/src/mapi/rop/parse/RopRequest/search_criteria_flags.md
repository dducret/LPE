---
type: Rust Method
title: search_criteria_flags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L620-L639
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
---

# Signature

`pub(in crate::mapi) fn search_criteria_flags(&self) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)