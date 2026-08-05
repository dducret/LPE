---
type: Rust Method
title: update_accessible_contact
resource: crates/lpe-exchange/src/tests/mod.rs#L8725-L8772
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn update_accessible_contact<'a>( &'a self, _principal_account_id: Uuid, contact_id: Uuid, input: UpsertClientContactInput, ) -> StoreFuture<'a, AccessibleContact>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)