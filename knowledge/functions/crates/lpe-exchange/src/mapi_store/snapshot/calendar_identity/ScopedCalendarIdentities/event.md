---
type: Rust Method
title: event
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L65-L72
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn event(&self, canonical_id: Uuid) -> Result<(u64, Vec<u8>)>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)