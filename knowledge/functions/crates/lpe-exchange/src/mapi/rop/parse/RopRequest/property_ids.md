---
type: Rust Method
title: property_ids
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1238-L1250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response
---

# Signature

`pub(in crate::mapi) fn property_ids(&self) -> Vec<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_names_from_property_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response.md)
- [rop_get_names_from_property_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response.md)