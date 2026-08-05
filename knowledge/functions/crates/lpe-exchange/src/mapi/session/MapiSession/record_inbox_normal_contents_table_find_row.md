---
type: Rust Method
title: record_inbox_normal_contents_table_find_row
resource: crates/lpe-exchange/src/mapi/session.rs#L202-L215
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
---

# Signature

`pub(in crate::mapi) fn record_inbox_normal_contents_table_find_row( &mut self, handle: Option<u32>, context: String, )`

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)