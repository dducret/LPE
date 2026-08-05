---
type: Rust Method
title: upsert_journal_entry
resource: crates/lpe-storage/src/notes_journal.rs#L417-L554
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  - functions/crates/lpe-storage/src/notes_journal/map_journal_entry
---

# Signature

`pub async fn upsert_journal_entry( &self, input: UpsertJournalEntryInput, ) -> Result<JournalEntry>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)
- [map_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/map_journal_entry.md)