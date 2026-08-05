---
type: Rust Method
title: set_mailbox_subscription
resource: crates/lpe-imap/src/tests.rs#L570-L584
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn set_mailbox_subscription<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, is_subscribed: bool, _audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`