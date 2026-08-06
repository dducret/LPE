---
type: Rust Module
title: hierarchy
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L1-L945
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-mapi-notifications-mapinotificationevent-mapinotificationkind
  - external/crate-mapi-wire-mapinotificationeventmask
  - external/crate-mapi-store-mapipublicfolder
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [HierarchyRow](../../../../../../classes/crates/lpe-exchange/src/mapi/tables/hierarchy/HierarchyRow.md)
- [hierarchy_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows.md)
- [hierarchy_rows_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [HierarchyTableRowModified](../../../../../../classes/crates/lpe-exchange/src/mapi/tables/hierarchy/HierarchyTableRowModified.md)
- [hierarchy_table_row_modified](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified.md)
- [hierarchy_depth_folder_ids_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_depth_folder_ids_excluding_deleted.md)
- [sort_hierarchy_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)
- [hierarchy_row_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_display_name.md)
- [mailbox_shadowed_by_active_outlook_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder.md)
- [collaboration_folder_shadows_outlook_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/collaboration_folder_shadows_outlook_special_folder.md)
- [hierarchy_row_content_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_content_count.md)
- [hierarchy_row_unread_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_unread_count.md)
- [hierarchy_row_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [hierarchy_row_parent_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id.md)
- [hierarchy_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)
- [hierarchy_row_folder_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags.md)
- [hierarchy_folder_is_in_ipm_subtree](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree.md)
- [hierarchy_row_expected_container_class](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_expected_container_class.md)
- [hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)
- [special_hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches.md)
- [log_sync_issues_hierarchy_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows.md)
- [special_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)
- [special_folder_property_value_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [serialize_hierarchy_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
- [serialize_hierarchy_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_property_row.md)
- [hierarchy_row_property_is_present](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)

# Imports

- `super::*`
- `crate::mapi::notifications::{MapiNotificationEvent, MapiNotificationKind}`
- `crate::mapi::wire::MapiNotificationEventMask`
- `crate::mapi_store::MapiPublicFolder`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)