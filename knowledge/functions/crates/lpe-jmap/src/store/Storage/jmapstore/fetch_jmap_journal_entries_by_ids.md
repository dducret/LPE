---
type: Rust Method
title: fetch_jmap_journal_entries_by_ids
resource: crates/lpe-jmap/src/store.rs#L1149-L1155
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn fetch_jmap_journal_entries_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<JournalEntry>>`