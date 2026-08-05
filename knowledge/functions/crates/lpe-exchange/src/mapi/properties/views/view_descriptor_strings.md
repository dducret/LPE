---
type: Rust Function
title: view_descriptor_strings
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L692-L700
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
---

# Signature

`pub(in crate::mapi) fn view_descriptor_strings(definition: &ViewDefinition) -> String`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [view_descriptor_strings_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary.md)
- [log_view_definition_diagnostics](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)