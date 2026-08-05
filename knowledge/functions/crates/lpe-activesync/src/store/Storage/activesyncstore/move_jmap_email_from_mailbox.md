---
type: Rust Method
title: move_jmap_email_from_mailbox
resource: crates/lpe-activesync/src/store.rs#L411-L429
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn move_jmap_email_from_mailbox<'a>( &'a self, account_id: Uuid, source_mailbox_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`