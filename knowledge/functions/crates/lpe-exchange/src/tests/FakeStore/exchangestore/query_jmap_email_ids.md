---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L10832-L10863
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/jmap_search_matches
---

# Signature

`fn query_jmap_email_ids<'a>( &'a self, _account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&'a str>, _position: u64, _limit: u64, ) -> StoreFuture<'a, JmapEmailQuery>`

# Calls

- [jmap_search_matches](../../../../../../../functions/crates/lpe-exchange/src/tests/jmap_search_matches.md)