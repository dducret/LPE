---
type: Rust Method
title: fetch_journal_entries_by_ids
resource: crates/lpe-storage/src/notes_journal.rs#L397-L415
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/notes_journal/journal_select_sql
---

# Signature

`pub async fn fetch_journal_entries_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<JournalEntry>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [journal_select_sql](../../../../../../functions/crates/lpe-storage/src/notes_journal/journal_select_sql.md)