---
type: Rust Function
title: query_rows_response_columns
resource: crates/lpe-exchange/src/mapi/tables/query.rs#L14-L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns
  - functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_calendar_configuration_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns
  - functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn query_rows_response_columns( object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, ) -> Vec<u32>`

# Calls

- [is_queryable_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [default_hierarchy_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns.md)
- [is_contact_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/is_contact_contents_folder.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [default_contact_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags.md)
- [default_navigation_shortcut_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags.md)
- [default_conversation_action_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags.md)
- [default_message_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)
- [default_calendar_configuration_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_calendar_configuration_property_tags.md)
- [should_use_associated_config_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table.md)
- [default_associated_config_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns.md)
- [default_contents_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns.md)
- [default_attachment_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns.md)
- [default_permission_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns.md)
- [default_rule_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)