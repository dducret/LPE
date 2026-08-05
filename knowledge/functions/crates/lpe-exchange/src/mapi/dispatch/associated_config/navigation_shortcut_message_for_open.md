---
type: Rust Function
title: navigation_shortcut_message_for_open
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L126-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_unbacked_default_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_default_navigation_shortcut_from_wrong_folder
---

# Signature

`pub(super) fn navigation_shortcut_message_for_open( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<crate::mapi_store::MapiNavigationShortcutMessage>`

# Calls

- [navigation_shortcut_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [common_views_open_rejects_unbacked_default_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_unbacked_default_navigation_shortcut.md)
- [common_views_open_rejects_default_navigation_shortcut_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_open_rejects_default_navigation_shortcut_from_wrong_folder.md)