---
type: Rust Method
title: update_accessible_event
resource: crates/lpe-exchange/src/tests/mod.rs#L8843-L8880
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn update_accessible_event<'a>( &'a self, _principal_account_id: Uuid, event_id: Uuid, input: UpsertClientEventInput, ) -> StoreFuture<'a, AccessibleEvent>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)