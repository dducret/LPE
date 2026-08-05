---
type: Rust Method
title: delete_jmap_email_from_mailbox
resource: crates/lpe-activesync/src/store.rs#L431-L442
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_jmap_email_from_mailbox<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`