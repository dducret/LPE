---
type: Rust Method
title: fetch_mapi_journal_entries
resource: crates/lpe-exchange/src/store.rs#L800-L808
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_mapi_journal_entries<'a>( &'a self, account_id: Uuid, ) -> StoreFuture<'a, Vec<JournalEntry>>`