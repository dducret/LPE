---
type: Rust Method
title: property_values
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1280-L1302
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(in crate::mapi) fn property_values(&self) -> Result<Vec<(u32, MapiValue)>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_tagged_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)

# Called by

- [set_properties_probe_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [append_set_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)