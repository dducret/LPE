---
type: Rust Method
title: fetch_jmap_emails
resource: crates/lpe-activesync/src/store.rs#L287-L293
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_jmap_emails<'a>( &'a self, account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<JmapEmail>>`