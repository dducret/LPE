---
type: Rust Method
title: fetch_mapi_journal_entries_by_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L9491-L9505
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_mapi_journal_entries_by_ids<'a>( &'a self, _account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<JournalEntry>>`