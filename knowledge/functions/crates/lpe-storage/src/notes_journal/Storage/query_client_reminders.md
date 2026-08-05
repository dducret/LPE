---
type: Rust Method
title: query_client_reminders
resource: crates/lpe-storage/src/notes_journal.rs#L611-L854
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn query_client_reminders( &self, account_id: Uuid, query: ReminderQuery, ) -> Result<Vec<ClientReminder>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)