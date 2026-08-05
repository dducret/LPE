---
type: Rust Function
title: format_inbox_associated_prefix_find_summary
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L895-L929
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn format_inbox_associated_prefix_find_summary( position: usize, sort_orders: &[MapiSortOrder], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [outlook_configuration_prefix_debug_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction.md)
- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [sort_associated_config_messages_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/sort_associated_config_messages_for_debug.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)