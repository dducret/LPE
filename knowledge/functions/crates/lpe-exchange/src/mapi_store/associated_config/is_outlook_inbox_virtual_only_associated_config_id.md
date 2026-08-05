---
type: Rust Function
title: is_outlook_inbox_virtual_only_associated_config_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L177-L189
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin
---

# Signature

`pub(crate) fn is_outlook_inbox_virtual_only_associated_config_id(item_id: u64) -> bool`

# Called by

- [associated_config_visible_in_table](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table.md)
- [fai_debug_item_classification](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification.md)
- [fai_debug_state_origin](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin.md)