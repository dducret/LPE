---
type: Rust Function
title: view_descriptor_runtime_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L801-L814
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
---

# Signature

`fn view_descriptor_runtime_property_tags(descriptor: &[u8]) -> Vec<u32>`

# Called by

- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [format_default_view_table_compatibility_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)