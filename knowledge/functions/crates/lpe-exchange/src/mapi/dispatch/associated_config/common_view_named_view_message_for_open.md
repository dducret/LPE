---
type: Rust Function
title: common_view_named_view_message_for_open
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L137-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_default_named_view_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_inbox_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_contact_view
---

# Signature

`pub(super) fn common_view_named_view_message_for_open( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage>`

# Calls

- [common_view_named_view_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [folder_local_default_named_view_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported.md)
- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [common_views_open_rejects_default_named_view_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_default_named_view_from_wrong_folder.md)
- [folder_default_named_view_open_rejects_unpersisted_inbox_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_inbox_view.md)
- [folder_default_named_view_open_rejects_unpersisted_contact_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_contact_view.md)