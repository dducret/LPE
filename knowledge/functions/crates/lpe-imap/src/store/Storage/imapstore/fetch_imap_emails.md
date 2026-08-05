---
type: Rust Method
title: fetch_imap_emails
resource: crates/lpe-imap/src/store.rs#L156-L162
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_imap_emails<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, ) -> StoreFuture<'a, Vec<ImapEmail>>`