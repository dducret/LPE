---
type: Rust Method
title: fetch_activesync_attachment_content
resource: crates/lpe-activesync/src/store.rs#L488-L497
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_attachment_content<'a>( &'a self, account_id: Uuid, file_reference: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>>`