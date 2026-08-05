---
type: Rust Function
title: and_restriction
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L799-L812
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop
---

# Signature

`fn and_restriction(mut restrictions: Vec<Vec<u8>>, force_wrapper: bool) -> Vec<u8>`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [bounded_search_criteria_to_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop.md)