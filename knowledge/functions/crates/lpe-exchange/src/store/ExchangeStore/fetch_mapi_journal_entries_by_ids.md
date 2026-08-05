---
type: Rust Method
title: fetch_mapi_journal_entries_by_ids
resource: crates/lpe-exchange/src/store.rs#L811-L820
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_mapi_journal_entries_by_ids<'a>( &'a self, account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<JournalEntry>>`