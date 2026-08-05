---
type: Rust Method
title: copy_imap_email
resource: crates/lpe-imap/src/store.rs#L270-L287
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn copy_imap_email<'a>( &'a self, account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, ImapEmail>`