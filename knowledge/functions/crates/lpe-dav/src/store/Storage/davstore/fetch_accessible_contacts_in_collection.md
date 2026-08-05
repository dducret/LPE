---
type: Rust Method
title: fetch_accessible_contacts_in_collection
resource: crates/lpe-dav/src/store.rs#L132-L141
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_accessible_contacts_in_collection<'a>( &'a self, principal_account_id: Uuid, collection_id: &'a str, ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>>`