---
type: Rust Function
title: view_descriptor_strings_binary
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L702-L709
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
---

# Signature

`pub(in crate::mapi) fn view_descriptor_strings_binary(definition: &ViewDefinition) -> Vec<u8>`

# Calls

- [view_descriptor_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)