---
type: Rust Method
title: fetch_client_events_by_ids
resource: crates/lpe-storage/src/workspace.rs#L874-L921
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_client_events_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientEvent>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)