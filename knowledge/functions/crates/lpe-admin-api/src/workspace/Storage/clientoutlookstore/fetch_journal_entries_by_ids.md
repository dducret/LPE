---
type: Rust Method
title: fetch_journal_entries_by_ids
resource: crates/lpe-admin-api/src/workspace.rs#L194-L200
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn fetch_journal_entries_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> anyhow::Result<Vec<JournalEntry>>`