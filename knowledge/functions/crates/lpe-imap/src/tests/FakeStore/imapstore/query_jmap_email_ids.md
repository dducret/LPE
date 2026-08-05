---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-imap/src/tests.rs#L395-L419
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn query_jmap_email_ids<'a>( &'a self, _account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&'a str>, _position: u64, _limit: u64, ) -> StoreFuture<'a, JmapEmailQuery>`