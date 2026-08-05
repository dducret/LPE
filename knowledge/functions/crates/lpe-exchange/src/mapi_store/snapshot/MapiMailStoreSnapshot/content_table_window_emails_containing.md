---
type: Rust Method
title: content_table_window_emails_containing
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L818-L852
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_skips_incomplete_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_boundary_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_longer_tail_window
---

# Signature

`pub(crate) fn content_table_window_emails_containing( &self, folder_id: u64, view_signature: u64, position: usize, ) -> Option<(usize, usize, Vec<&JmapEmail>)>`

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [content_table_window_emails_containing_skips_incomplete_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_skips_incomplete_window.md)
- [content_table_window_emails_containing_prefers_boundary_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_boundary_window.md)
- [content_table_window_emails_containing_prefers_longer_tail_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_longer_tail_window.md)