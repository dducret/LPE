---
type: Rust Function
title: set_properties_problem_count
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L300-L307
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract
---

# Signature

`fn set_properties_problem_count(response: &[u8]) -> usize`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [post_hierarchy_setprops_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract.md)