---
type: Rust Function
title: table_row_keys
resource: crates/lpe-exchange/src/mapi/tables/row_keys.rs#L3-L254
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks
  - functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages
  - functions/crates/lpe-exchange/src/mapi/tables/filters/retain_rows_by_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items
  - functions/crates/lpe-exchange/src/mapi/tables/counts/normal_contents_suppressed_for_associated_only_folder
  - functions/crates/lpe-exchange/src/mapi/tables/filters/is_top_level_count_restriction
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_window_emails_containing
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_total
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
---

# Signature

`pub(in crate::mapi) fn table_row_keys( object: &MapiObject, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u64>`

# Calls

- [is_queryable_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [deleted_items_content_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_id.md)
- [calendar_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows.md)
- [sort_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [contacts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder.md)
- [restriction_matches_contact_in_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [sort_contacts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts.md)
- [tasks_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder.md)
- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [sort_tasks](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks.md)
- [is_contact_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder.md)
- [notes_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder.md)
- [restriction_matches_note](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note.md)
- [sort_notes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes.md)
- [contacts_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results.md)
- [delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages.md)
- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [delegate_freebusy_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value.md)
- [todo_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows.md)
- [sort_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows.md)
- [search_content_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_id.md)
- [tracked_mail_processing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages.md)
- [retain_rows_by_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/retain_rows_by_restriction.md)
- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)
- [reminder_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows.md)
- [journal_entries_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder.md)
- [restriction_matches_journal_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry.md)
- [sort_journal_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [recoverable_items_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder.md)
- [sort_recoverable_items](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items.md)
- [normal_contents_suppressed_for_associated_only_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/normal_contents_suppressed_for_associated_only_folder.md)
- [is_top_level_count_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/is_top_level_count_restriction.md)
- [content_table_window_emails_containing](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_window_emails_containing.md)
- [table_view_signature](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature.md)
- [content_table_total](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_total.md)
- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [restriction_matches_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)
- [sort_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments.md)
- [permissions_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [rules](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules.md)

# Called by

- [rop_create_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response.md)
- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)