---
type: Rust Method
title: rename_imap_mailbox
resource: crates/lpe-imap/src/store.rs#L226-L237
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn rename_imap_mailbox<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, name: &'a str, audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`