---
type: Rust Function
title: format_calendar_required_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar.rs#L249-L281
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/calendar_configuration_debug_contract_uses_roaming_properties
---

# Signature

`pub(in crate::mapi::dispatch) fn format_calendar_required_property_tags( has_configuration_objects: bool, has_appointment_objects: bool, ) -> String`

# Calls

- [format_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)

# Called by

- [calendar_configuration_debug_contract_uses_roaming_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/calendar_configuration_debug_contract_uses_roaming_properties.md)