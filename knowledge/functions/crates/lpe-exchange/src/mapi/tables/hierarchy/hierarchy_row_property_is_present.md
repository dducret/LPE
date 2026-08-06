---
type: Rust Function
title: hierarchy_row_property_is_present
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L862-L889
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_property_row
---

# Signature

`fn hierarchy_row_property_is_present( row: &HierarchyRow<'_>, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, property_tag: u32, mailbox_guid: Uuid, ) -> bool`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [hierarchy_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)

# Called by

- [serialize_hierarchy_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_property_row.md)