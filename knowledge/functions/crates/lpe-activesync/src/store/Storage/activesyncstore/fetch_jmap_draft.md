---
type: Rust Method
title: fetch_jmap_draft
resource: crates/lpe-activesync/src/store.rs#L395-L401
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_jmap_draft<'a>( &'a self, account_id: Uuid, id: Uuid, ) -> StoreFuture<'a, Option<JmapEmail>>`