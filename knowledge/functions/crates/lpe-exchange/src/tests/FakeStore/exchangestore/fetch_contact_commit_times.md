---
type: Rust Method
title: fetch_contact_commit_times
resource: crates/lpe-exchange/src/tests/mod.rs#L7821-L7835
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`fn fetch_contact_commit_times<'a>( &'a self, _principal_account_id: Uuid, collection_id: &'a str, ) -> StoreFuture<'a, Vec<(Uuid, String)>>`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)