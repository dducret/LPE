---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-imap/src/store.rs#L201-L213
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn query_jmap_email_ids<'a>( &'a self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&'a str>, position: u64, limit: u64, ) -> StoreFuture<'a, JmapEmailQuery>`