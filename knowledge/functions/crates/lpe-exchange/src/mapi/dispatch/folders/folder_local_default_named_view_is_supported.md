---
type: Rust Function
title: folder_local_default_named_view_is_supported
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1396-L1413
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_uses_common_views
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
---

# Signature

`pub(super) fn folder_local_default_named_view_is_supported( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> bool`

# Calls

- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [advertised_special_folder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_view_uses_common_views](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_uses_common_views.md)

# Called by

- [common_view_named_view_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)