---
type: Rust Method
title: object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L127-L133
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
---

# Signature

`fn object_id(&self, canonical_id: &Uuid) -> Option<u64>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)