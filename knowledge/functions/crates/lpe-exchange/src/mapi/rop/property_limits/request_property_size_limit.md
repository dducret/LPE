---
type: Rust Function
title: request_property_size_limit
resource: crates/lpe-exchange/src/mapi/rop/property_limits.rs#L136-L144
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
---

# Signature

`pub(super) fn request_property_size_limit(request: &RopRequest) -> usize`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [size_limited_specific_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)