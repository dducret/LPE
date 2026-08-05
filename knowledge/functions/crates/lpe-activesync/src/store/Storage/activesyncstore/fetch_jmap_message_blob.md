---
type: Rust Method
title: fetch_jmap_message_blob
resource: crates/lpe-activesync/src/store.rs#L403-L409
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_jmap_message_blob<'a>( &'a self, account_id: Uuid, message_id: Uuid, ) -> StoreFuture<'a, Option<JmapUploadBlob>>`