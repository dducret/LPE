---
type: Rust Function
title: apply_sort_direction
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L4-L10
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries
---

# Signature

`pub(in crate::mapi) fn apply_sort_direction(ordering: Ordering, sort_order: u8) -> Ordering`

# Called by

- [sort_associated_config_messages_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug.md)
- [sort_debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)
- [sort_hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)
- [sort_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)
- [sort_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows.md)
- [sort_recoverable_items](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items.md)
- [sort_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments.md)
- [sort_contacts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts.md)
- [sort_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events.md)
- [sort_tasks](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks.md)
- [sort_notes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes.md)
- [sort_journal_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries.md)