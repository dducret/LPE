---
type: Rust Function
title: outlook_inbox_associated_config_defaults
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L248-L352
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_synthetic_inbox_associated_config
---

# Signature

`pub(super) fn outlook_inbox_associated_config_defaults( folder_id: u64, ) -> Vec<MapiAssociatedConfigMessage>`

# Called by

- [outlook_inbox_exact_virtual_associated_config_for_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class.md)
- [outlook_inbox_exact_virtual_associated_config_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id.md)
- [is_empty_synthetic_inbox_associated_config](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_synthetic_inbox_associated_config.md)