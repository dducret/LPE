---
type: Rust Method
title: normalize_named_property_tag
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L111-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request
---

# Signature

`pub(in crate::mapi) fn normalize_named_property_tag(&self, property_tag: u32) -> u32`

# Calls

- [property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [property_type_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [append_open_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_set_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_delete_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [normalized_get_properties_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request.md)