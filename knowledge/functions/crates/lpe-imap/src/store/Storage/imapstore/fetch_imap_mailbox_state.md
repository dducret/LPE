---
type: Rust Method
title: fetch_imap_mailbox_state
resource: crates/lpe-imap/src/store.rs#L148-L154
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_imap_mailbox_state<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, ) -> StoreFuture<'a, ImapMailboxState>`