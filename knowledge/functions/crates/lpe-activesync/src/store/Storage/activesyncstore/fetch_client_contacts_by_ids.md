---
type: Rust Method
title: fetch_client_contacts_by_ids
resource: crates/lpe-activesync/src/store.rs#L534-L540
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_client_contacts_by_ids<'a>( &'a self, account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<ClientContact>>`