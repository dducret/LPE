---
type: Rust Method
title: sort_category_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1153-L1159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid
---

# Signature

`pub(in crate::mapi) fn sort_category_count(&self) -> u16`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_sort_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [sort_table_request_is_valid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid.md)