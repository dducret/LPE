---
type: Rust Method
title: named_properties_for_query
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L123-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response
---

# Signature

`pub(in crate::mapi) fn named_properties_for_query( &self, guid: Option<[u8; 16]>, ) -> Vec<(u16, MapiNamedProperty)>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [calendar_named_property_mapping_keeps_registered_database_ids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids.md)
- [rop_query_named_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response.md)