---
type: Rust Function
title: outlook_inbox_associated_config_sync_defaults
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L354-L359
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
---

# Signature

`pub(super) fn outlook_inbox_associated_config_sync_defaults( folder_id: u64, ) -> Vec<MapiAssociatedConfigMessage>`

# Called by

- [modeled_virtual_associated_config_message_for_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id.md)
- [associated_config_messages_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)