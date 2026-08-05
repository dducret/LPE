---
type: Rust Method
title: create_accessible_event
resource: crates/lpe-dav/src/store.rs#L188-L198
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn create_accessible_event<'a>( &'a self, principal_account_id: Uuid, collection_id: Option<&'a str>, input: UpsertClientEventInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent>`