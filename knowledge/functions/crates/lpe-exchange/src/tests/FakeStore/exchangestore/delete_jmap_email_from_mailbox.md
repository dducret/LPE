---
type: Rust Method
title: delete_jmap_email_from_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L11628-L11644
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_jmap_email_from_mailbox<'a>( &'a self, account_id: Uuid, _mailbox_id: Uuid, message_id: Uuid, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`