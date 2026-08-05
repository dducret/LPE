---
type: Rust Method
title: message_identity
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L74-L76
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`fn message_identity(&self, canonical_id: Uuid) -> Option<&MapiIdentityRecord>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [build](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)