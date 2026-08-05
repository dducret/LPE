---
type: Rust Method
title: delete_imap_mailbox
resource: crates/lpe-imap/src/store.rs#L239-L249
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_imap_mailbox<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`