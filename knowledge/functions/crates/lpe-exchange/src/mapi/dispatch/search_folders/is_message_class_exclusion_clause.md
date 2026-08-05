---
type: Rust Function
title: is_message_class_exclusion_clause
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L794-L797
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop
---

# Signature

`fn is_message_class_exclusion_clause(clause: &Value) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [bounded_search_criteria_to_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop.md)