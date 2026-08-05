---
type: Rust Method
title: named_property_names
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1256-L1271
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`pub(in crate::mapi) fn named_property_names(&self) -> Result<Vec<MapiNamedProperty>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)