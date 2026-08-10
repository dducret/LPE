---
type: Rust Function
title: append_debug_modeled_inbox_exact_startup_config
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1422-L1435
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config
---

# Signature

`fn append_debug_modeled_inbox_exact_startup_config( messages: &mut Vec<crate::mapi_store::MapiAssociatedConfigMessage>, message: Option<crate::mapi_store::MapiAssociatedConfigMessage>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_exact_virtual_inbox_debug_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config.md)