---
type: Rust Method
title: fetch_client_notes
resource: crates/lpe-storage/src/notes_journal.rs#L159-L182
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_client_notes(&self, account_id: Uuid) -> Result<Vec<ClientNote>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)