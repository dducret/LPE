---
type: Rust Function
title: restricted_associated_folder_message_count
resource: crates/lpe-exchange/src/mapi/tables/counts.rs#L183-L219
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_clamps_stale_cursor_to_current_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count
---

# Signature

`pub(in crate::mapi) fn restricted_associated_folder_message_count( folder_id: u64, snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> usize`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [restriction_matches_common_views_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [conversation_action_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages.md)
- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [conversation_action_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages.md)
- [delegate_freebusy_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value.md)
- [associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows.md)

# Called by

- [outlook_bootstrap_query_rows_total_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count.md)
- [log_outlook_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [query_position_clamps_stale_cursor_to_current_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_clamps_stale_cursor_to_current_row_count.md)
- [restricted_associated_query_position_reports_filtered_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count.md)