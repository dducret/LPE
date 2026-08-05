---
type: Rust Method
title: fetch_mapi_navigation_shortcuts
resource: crates/lpe-exchange/src/tests/mod.rs#L9680-L9686
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`fn fetch_mapi_navigation_shortcuts<'a>( &'a self, _account_id: Uuid, ) -> StoreFuture<'a, Vec<crate::store::MapiNavigationShortcutRecord>>`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)