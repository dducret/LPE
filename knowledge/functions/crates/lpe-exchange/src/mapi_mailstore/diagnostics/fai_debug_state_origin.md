---
type: Rust Function
title: fai_debug_state_origin
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L472-L498
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug
---

# Signature

`pub(crate) fn fai_debug_state_origin( folder_id: u64, special_object: Option<&SpecialMessageSyncFact>, item_id: u64, ) -> &'static str`

# Calls

- [is_outlook_inbox_virtual_only_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [log_fai_content_sync_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug.md)