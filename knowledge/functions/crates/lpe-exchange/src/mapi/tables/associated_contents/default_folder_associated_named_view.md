---
type: Rust Function
title: default_folder_associated_named_view
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L242-L263
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
---

# Signature

`fn default_folder_associated_named_view( snapshot: &MapiMailStoreSnapshot, folder_id: u64, ) -> Option<MapiCommonViewNamedViewMessage>`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_common_views_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)
- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [outlook_default_folder_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)

# Called by

- [has_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows.md)
- [associated_table_rows_with_lookup_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)