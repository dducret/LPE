---
type: Rust Function
title: outlook_inbox_exact_virtual_associated_config_for_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L381-L393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
---

# Signature

`pub(super) fn outlook_inbox_exact_virtual_associated_config_for_id( item_id: u64, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [outlook_inbox_associated_config_defaults](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults.md)
- [outlook_inbox_exact_virtual_associated_config_for_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class.md)

# Called by

- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)