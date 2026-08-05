---
type: Rust Function
title: serialize_advertised_special_folder_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L302-L308
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
---

# Signature

`pub(in crate::mapi) fn serialize_advertised_special_folder_row_with_mailbox_guid( folder_id: u64, columns: &[u32], mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [serialize_advertised_special_folder_row_with_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts.md)

# Called by

- [serialize_advertised_special_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row.md)
- [ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders.md)