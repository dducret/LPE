---
type: Rust Method
title: fetch_contact_sync_versions
resource: crates/lpe-exchange/src/tests/mod.rs#L7795-L7815
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
---

# Signature

`fn fetch_contact_sync_versions<'a>( &'a self, _principal_account_id: Uuid, collection_id: &'a str, ) -> StoreFuture<'a, Vec<(Uuid, String)>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [contact_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)