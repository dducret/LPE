---
type: Rust Function
title: request_get_properties_all_want_unicode
resource: crates/lpe-exchange/src/mapi/rop.rs#L1133-L1141
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
---

# Signature

`fn request_get_properties_all_want_unicode(request: &RopRequest) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)