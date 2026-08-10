---
type: Rust Method
title: move_jmap_email_from_mailbox
resource: crates/lpe-activesync/src/tests.rs#L655-L676
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn move_jmap_email_from_mailbox<'a>( &'a self, _account_id: Uuid, source_mailbox_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`