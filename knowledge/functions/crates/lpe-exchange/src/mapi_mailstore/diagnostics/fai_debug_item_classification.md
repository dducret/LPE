---
type: Rust Function
title: fai_debug_item_classification
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L444-L470
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug
---

# Signature

`fn fai_debug_item_classification( folder_id: u64, special_object: Option<&SpecialMessageSyncFact>, item_id: u64, ) -> &'static str`

# Calls

- [is_outlook_inbox_virtual_only_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)

# Called by

- [log_fai_content_sync_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug.md)