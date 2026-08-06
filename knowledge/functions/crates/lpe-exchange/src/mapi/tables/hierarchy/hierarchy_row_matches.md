---
type: Rust Function
title: hierarchy_row_matches
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L499-L522
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
---

# Signature

`pub(super) fn hierarchy_row_matches( row: &HierarchyRow<'_>, mailboxes: &[JmapMailbox], restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches_mailbox_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account.md)
- [restriction_matches_collaboration_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder.md)
- [restriction_matches_public_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder.md)
- [special_hierarchy_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches.md)

# Called by

- [find_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)