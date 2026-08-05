---
type: Rust Method
title: fetch_client_contacts
resource: crates/lpe-storage/src/workspace.rs#L902-L954
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_client_contacts(&self, account_id: Uuid) -> Result<Vec<ClientContact>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)