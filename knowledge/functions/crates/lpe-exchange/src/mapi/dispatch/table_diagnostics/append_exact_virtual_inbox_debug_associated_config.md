---
type: Rust Function
title: append_exact_virtual_inbox_debug_associated_config
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1390-L1410
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_exact_message_class_restriction_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_debug_modeled_inbox_exact_startup_config
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
---

# Signature

`fn append_exact_virtual_inbox_debug_associated_config( folder_id: u64, restriction: Option<&MapiRestriction>, messages: &mut Vec<crate::mapi_store::MapiAssociatedConfigMessage>, )`

# Calls

- [debug_exact_message_class_restriction_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_exact_message_class_restriction_value.md)
- [append_debug_modeled_inbox_exact_startup_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_debug_modeled_inbox_exact_startup_config.md)
- [outlook_inbox_exact_virtual_associated_config_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class.md)

# Called by

- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)