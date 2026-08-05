---
type: Rust Function
title: format_debug_named_property_sample
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L657-L671
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_sample_is_bounded
---

# Signature

`pub(super) fn format_debug_named_property_sample( properties: &[MapiNamedProperty], limit: usize, ) -> String`

# Calls

- [format_debug_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [record_post_calendar_query_position_named_property_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe.md)
- [debug_named_property_sample_is_bounded](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_sample_is_bounded.md)