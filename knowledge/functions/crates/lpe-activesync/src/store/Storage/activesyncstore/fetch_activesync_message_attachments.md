---
type: Rust Method
title: fetch_activesync_message_attachments
resource: crates/lpe-activesync/src/store.rs#L477-L486
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_message_attachments<'a>( &'a self, account_id: Uuid, message_id: Uuid, ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>>`