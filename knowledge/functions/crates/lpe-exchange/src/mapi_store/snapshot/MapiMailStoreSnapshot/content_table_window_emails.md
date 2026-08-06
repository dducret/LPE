---
type: Rust Method
title: content_table_window_emails
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L807-L848
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_reuses_wider_window_slice
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_skips_insufficient_containing_window
---

# Signature

`pub(crate) fn content_table_window_emails( &self, folder_id: u64, view_signature: u64, offset: usize, limit: usize, ) -> Option<(usize, Vec<&JmapEmail>)>`

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [content_table_window_emails_reuses_wider_window_slice](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_reuses_wider_window_slice.md)
- [content_table_window_emails_skips_insufficient_containing_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_skips_insufficient_containing_window.md)