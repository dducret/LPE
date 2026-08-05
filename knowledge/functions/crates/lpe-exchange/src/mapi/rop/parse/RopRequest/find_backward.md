---
type: Rust Method
title: find_backward
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1084-L1086
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/tables/find/find_row
  - functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row
---

# Signature

`pub(in crate::mapi) fn find_backward(&self) -> bool`

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/find/find_row.md)
- [find_hierarchy_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row.md)