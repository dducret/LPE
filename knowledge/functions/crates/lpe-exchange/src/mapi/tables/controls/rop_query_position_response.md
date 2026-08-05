---
type: Rust Function
title: rop_query_position_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L81-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_position_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_clamps_stale_cursor_to_current_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_counts_categorized_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp
---

# Signature

`pub(in crate::mapi) fn rop_query_position_response( request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)

# Called by

- [query_position_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_position_response.md)
- [default_contacts_contents_table_uses_contact_rows_and_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns.md)
- [query_rows_ignores_incomplete_windowed_content_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows.md)
- [query_position_clamps_stale_cursor_to_current_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_clamps_stale_cursor_to_current_row_count.md)
- [restricted_associated_query_position_reports_filtered_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count.md)
- [captured_calendar_table_query_rows_projects_exact_requested_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row.md)
- [query_position_counts_categorized_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_counts_categorized_content_rows.md)
- [contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp.md)