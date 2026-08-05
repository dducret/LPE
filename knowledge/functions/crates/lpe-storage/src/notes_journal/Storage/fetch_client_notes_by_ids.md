---
type: Rust Method
title: fetch_client_notes_by_ids
resource: crates/lpe-storage/src/notes_journal.rs#L184-L217
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_client_notes_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientNote>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)