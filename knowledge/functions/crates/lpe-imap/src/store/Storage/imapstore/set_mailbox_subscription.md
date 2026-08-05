---
type: Rust Method
title: set_mailbox_subscription
resource: crates/lpe-imap/src/store.rs#L251-L268
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn set_mailbox_subscription<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, is_subscribed: bool, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`