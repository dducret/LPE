---
type: Rust Function
title: table_position_and_count
resource: crates/lpe-exchange/src/mapi/tables/counts.rs#L221-L529
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/counts/normal_contents_suppressed_for_associated_only_folder
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  - functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages
  - functions/crates/lpe-exchange/src/mapi/tables/filters/retain_rows_by_restriction
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/filters/is_top_level_count_restriction
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_total
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_position_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_tracks_cursor_boundary
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_clamps_stale_cursor_to_current_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_clamps_stale_current_position_to_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
---

# Signature

`pub(in crate::mapi) fn table_position_and_count( object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> (usize, usize)`

# Calls

- [is_queryable_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [normal_contents_suppressed_for_associated_only_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/normal_contents_suppressed_for_associated_only_folder.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)
- [default_contents_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns.md)
- [calendar_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [contacts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder.md)
- [restriction_matches_contact_in_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [tasks_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder.md)
- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [is_contact_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder.md)
- [notes_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder.md)
- [restriction_matches_note](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note.md)
- [contacts_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results.md)
- [todo_search_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages.md)
- [retain_rows_by_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/retain_rows_by_restriction.md)
- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [todo_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results.md)
- [tracked_mail_processing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages.md)
- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [reminder_tasks](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks.md)
- [reminder_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages.md)
- [journal_entries_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder.md)
- [restriction_matches_journal_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_items_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder.md)
- [restriction_matches_public_folder_item](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [recoverable_items_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder.md)
- [restriction_matches_conversation_member_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot.md)
- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [is_top_level_count_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/is_top_level_count_restriction.md)
- [content_table_total](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_total.md)
- [table_view_signature](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature.md)
- [restriction_matches_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)
- [permissions_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder.md)
- [rules](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules.md)

# Called by

- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [rop_query_position_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_position_response.md)
- [rop_seek_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response.md)
- [rop_seek_row_fractional_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [query_rows_origin_tracks_cursor_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_tracks_cursor_boundary.md)
- [query_rows_clamps_stale_cursor_to_current_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_clamps_stale_cursor_to_current_row_count.md)
- [seek_row_clamps_stale_current_position_to_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_clamps_stale_current_position_to_row_count.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)