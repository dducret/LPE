---
type: Rust Method
title: property_name_for_id
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L100-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response
---

# Signature

`pub(in crate::mapi) fn property_name_for_id(&self, property_id: u16) -> MapiNamedProperty`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [well_known_named_property_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id.md)

# Called by

- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [rop_get_names_from_property_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response.md)