---
type: Rust Function
title: compare_case_insensitive
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L12-L14
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/compare_debug_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values
---

# Signature

`pub(in crate::mapi) fn compare_case_insensitive(left: &str, right: &str) -> Ordering`

# Called by

- [sort_associated_config_messages_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug.md)
- [compare_debug_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/compare_debug_mapi_values.md)
- [compare_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)
- [sort_hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)
- [sort_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)
- [sort_recoverable_items](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_recoverable_items.md)
- [sort_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_attachments.md)
- [sort_contacts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts.md)
- [sort_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events.md)
- [sort_tasks](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_tasks.md)
- [sort_notes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_notes.md)
- [sort_journal_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries.md)
- [compare_optional_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values.md)