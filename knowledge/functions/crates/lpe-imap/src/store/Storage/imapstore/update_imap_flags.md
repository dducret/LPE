---
type: Rust Method
title: update_imap_flags
resource: crates/lpe-imap/src/store.rs#L164-L186
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_imap_flags<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, message_ids: &'a [Uuid], unread: Option<bool>, flagged: Option<bool>, deleted: Option<bool>, unchanged_since: Option<u64>, ) -> StoreFuture<'a, Vec<Uuid>>`