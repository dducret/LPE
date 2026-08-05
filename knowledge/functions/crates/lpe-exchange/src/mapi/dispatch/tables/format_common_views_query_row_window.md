---
type: Rust Function
title: format_common_views_query_row_window
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L388-L447
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window
---

# Signature

`pub(super) fn format_common_views_query_row_window( position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, _columns: &[u32], account_id: Uuid, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [restriction_matches_common_views_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)

# Called by

- [format_outlook_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window.md)