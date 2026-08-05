---
type: Rust Function
title: format_debug_named_properties
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties.rs#L11-L25
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_named_property_sample
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates
---

# Signature

`pub(in crate::mapi::dispatch) fn format_debug_named_properties( properties: &[MapiNamedProperty], ) -> String`

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [format_debug_named_property_sample](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_named_property_sample.md)
- [summarize_named_property_id_duplicates](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates.md)