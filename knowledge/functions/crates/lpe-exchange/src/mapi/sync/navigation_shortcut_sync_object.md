---
type: Rust Function
title: navigation_shortcut_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L669-L731
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`fn navigation_shortcut_sync_object( message: &crate::mapi_store::MapiNavigationShortcutMessage, principal: &AccountPrincipal, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [navigation_shortcut_property_value_for_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [common_views_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)
- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)