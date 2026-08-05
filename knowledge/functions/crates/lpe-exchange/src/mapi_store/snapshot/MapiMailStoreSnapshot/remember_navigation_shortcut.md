---
type: Rust Method
title: remember_navigation_shortcut
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L327-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
---

# Signature

`pub(crate) fn remember_navigation_shortcut( &mut self, shortcut: MapiNavigationShortcutRecord, identity: MapiIdentityRecord, ) -> Result<()>`

# Calls

- [navigation_shortcut_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)