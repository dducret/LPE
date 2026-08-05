---
type: Rust Function
title: serialize_hierarchy_row
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L696-L733
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
  - functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
---

# Signature

`pub(super) fn serialize_hierarchy_row( row: HierarchyRow<'_>, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, columns: &[u32], mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [folder_local_commit_time_max](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [hierarchy_table_row_modified](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders.md)
- [dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)