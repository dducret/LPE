---
type: Rust Method
title: move_jmap_email_from_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L11278-L11287
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn move_jmap_email_from_mailbox<'a>( &'a self, account_id: Uuid, _source_mailbox_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`