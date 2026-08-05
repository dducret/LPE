---
type: Rust Method
title: create_accessible_contact
resource: crates/lpe-dav/src/store.rs#L176-L186
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn create_accessible_contact<'a>( &'a self, principal_account_id: Uuid, collection_id: Option<&'a str>, input: UpsertClientContactInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact>`