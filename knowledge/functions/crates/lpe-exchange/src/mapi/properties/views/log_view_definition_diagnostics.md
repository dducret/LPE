---
type: Rust Function
title: log_view_definition_diagnostics
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L711-L733
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
---

# Signature

`pub(in crate::mapi) fn log_view_definition_diagnostics( folder_id: u64, view_id: u64, view_name: &str, definition: &ViewDefinition, )`

# Calls

- [view_descriptor_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)

# Called by

- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)