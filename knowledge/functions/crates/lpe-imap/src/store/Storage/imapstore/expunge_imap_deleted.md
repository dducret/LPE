---
type: Rust Method
title: expunge_imap_deleted
resource: crates/lpe-imap/src/store.rs#L188-L199
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn expunge_imap_deleted<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, message_ids: &'a [Uuid], audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`