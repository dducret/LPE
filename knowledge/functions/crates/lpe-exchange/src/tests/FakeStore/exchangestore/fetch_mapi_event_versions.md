---
type: Rust Method
title: fetch_mapi_event_versions
resource: crates/lpe-exchange/src/tests/mod.rs#L7883-L7922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
---

# Signature

`fn fetch_mapi_event_versions<'a>( &'a self, _principal_account_id: Uuid, event_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<MapiEventVersion>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)