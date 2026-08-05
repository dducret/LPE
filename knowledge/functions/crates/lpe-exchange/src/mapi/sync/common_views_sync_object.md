---
type: Rust Function
title: common_views_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L733-L751
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn common_views_sync_object( message: crate::mapi_store::MapiCommonViewsMessage, principal: &AccountPrincipal, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [navigation_shortcut_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [common_view_named_view_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [search_folder_definition_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)
- [associated_config_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)