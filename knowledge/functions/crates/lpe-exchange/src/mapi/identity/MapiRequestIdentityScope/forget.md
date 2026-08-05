---
type: Rust Method
title: forget
resource: crates/lpe-exchange/src/mapi/identity.rs#L120-L125
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity
---

# Signature

`fn forget(&self, canonical_id: &Uuid)`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [forget_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)