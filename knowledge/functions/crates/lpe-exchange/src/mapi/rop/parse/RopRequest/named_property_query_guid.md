---
type: Rust Method
title: named_property_query_guid
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1273-L1278
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response
---

# Signature

`pub(in crate::mapi) fn named_property_query_guid(&self) -> Option<[u8; 16]>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_query_named_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response.md)
- [rop_query_named_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response.md)