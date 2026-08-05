---
type: Rust Function
title: navigation_shortcut_message
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L24-L80
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/normalize_navigation_shortcut_group_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut
---

# Signature

`fn navigation_shortcut_message( shortcut: MapiNavigationShortcutRecord, durable_identity: Option<MapiIdentityRecord>, ) -> Result<MapiNavigationShortcutMessage>`

# Calls

- [normalize_navigation_shortcut_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi_store/normalize_navigation_shortcut_group_name.md)

# Called by

- [with_navigation_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts.md)
- [remember_navigation_shortcut](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut.md)