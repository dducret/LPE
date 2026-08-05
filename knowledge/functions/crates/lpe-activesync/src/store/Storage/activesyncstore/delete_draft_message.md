---
type: Rust Method
title: delete_draft_message
resource: crates/lpe-activesync/src/store.rs#L507-L517
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_draft_message<'a>( &'a self, account_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`