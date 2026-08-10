---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-activesync/src/tests.rs#L458-L490
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn query_jmap_email_ids<'a>( &'a self, _account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&'a str>, position: u64, limit: u64, ) -> StoreFuture<'a, JmapEmailQuery>`