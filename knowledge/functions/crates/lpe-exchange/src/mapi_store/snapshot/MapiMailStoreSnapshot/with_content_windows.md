---
type: Rust Method
title: with_content_windows
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L694-L700
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_uses_global_position_for_windowed_content_tables
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_reuses_wider_window_slice
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_skips_insufficient_containing_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_skips_incomplete_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_boundary_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_longer_tail_window
  - functions/crates/lpe-exchange/src/mapi_store/tests/content_table_total_survives_total_only_window_without_rows
---

# Signature

`pub(crate) fn with_content_windows( mut self, content_windows: Vec<MapiContentTableWindow>, ) -> Self`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [query_rows_origin_uses_global_position_for_windowed_content_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_uses_global_position_for_windowed_content_tables.md)
- [query_rows_ignores_incomplete_windowed_content_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows.md)
- [bookmark_seek_preserves_global_position_for_windowed_content_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables.md)
- [bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted.md)
- [find_row_uses_windowed_content_table_rows_with_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position.md)
- [find_row_beginning_origin_keeps_windowed_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position.md)
- [find_row_beginning_origin_falls_back_when_complete_rows_are_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded.md)
- [content_table_window_emails_reuses_wider_window_slice](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_reuses_wider_window_slice.md)
- [content_table_window_emails_skips_insufficient_containing_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_skips_insufficient_containing_window.md)
- [content_table_window_emails_containing_skips_incomplete_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_skips_incomplete_window.md)
- [content_table_window_emails_containing_prefers_boundary_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_boundary_window.md)
- [content_table_window_emails_containing_prefers_longer_tail_window](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_window_emails_containing_prefers_longer_tail_window.md)
- [content_table_total_survives_total_only_window_without_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/content_table_total_survives_total_only_window_without_rows.md)