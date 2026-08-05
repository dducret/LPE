---
type: Rust Method
title: expunge_imap_deleted
resource: crates/lpe-imap/src/tests.rs#L381-L393
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn expunge_imap_deleted<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, message_ids: &'a [Uuid], _audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`