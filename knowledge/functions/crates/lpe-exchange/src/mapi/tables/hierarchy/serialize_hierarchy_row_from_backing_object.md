---
type: Rust Function
title: serialize_hierarchy_row_from_backing_object
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L647-L701
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
---

# Signature

`fn serialize_hierarchy_row_from_backing_object( row: HierarchyRow<'_>, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, columns: &[u32], mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [serialize_folder_row_with_context_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [serialize_public_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row.md)
- [serialize_advertised_special_folder_row_with_counts_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version.md)
- [emails](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)

# Called by

- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)