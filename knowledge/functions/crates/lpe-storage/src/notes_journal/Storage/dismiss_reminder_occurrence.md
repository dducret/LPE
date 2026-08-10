---
type: Rust Method
title: dismiss_reminder_occurrence
resource: crates/lpe-storage/src/notes_journal.rs#L125-L157
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`pub async fn dismiss_reminder_occurrence( &self, account_id: Uuid, source_type: &str, source_id: Uuid, occurrence_start_at: &str, dismissed_at: &str, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)