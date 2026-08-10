---
type: Rust Function
title: expanded_categorized_rows
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L238-L278
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response
---

# Signature

`fn expanded_categorized_rows( folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, restriction: Option<&MapiRestriction>, columns: &[u32], sort_orders: &[MapiSortOrder], ) -> Vec<CategorizedTableRow>`

# Calls

- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)
- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)

# Called by

- [rop_expand_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response.md)
- [rop_collapse_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response.md)