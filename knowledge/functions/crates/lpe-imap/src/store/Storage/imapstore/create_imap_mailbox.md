---
type: Rust Method
title: create_imap_mailbox
resource: crates/lpe-imap/src/store.rs#L215-L224
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn create_imap_mailbox<'a>( &'a self, account_id: Uuid, name: &'a str, audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`