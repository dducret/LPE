---
type: Rust Function
title: collaboration_folder_tables_follow_effective_collection_grants
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L9612-L9716
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
---

# Signature

`fn collaboration_folder_tables_follow_effective_collection_grants()`

# Calls

- [collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [owner_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [rights_from_grant](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)
- [serialize_collaboration_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)