---
type: Rust Function
title: persisted_common_views_shortcuts
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L197-L250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_ordinal_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id
---

# Signature

`fn persisted_common_views_shortcuts( account_id: Uuid, ) -> Vec<crate::store::MapiNavigationShortcutRecord>`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [wlink_ordinal_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_ordinal_bytes.md)
- [default_wlink_group_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id.md)