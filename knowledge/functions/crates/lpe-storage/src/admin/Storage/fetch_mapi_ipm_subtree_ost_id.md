---
type: Rust Method
title: fetch_mapi_ipm_subtree_ost_id
resource: crates/lpe-storage/src/admin.rs#L272-L286
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_mapi_ipm_subtree_ost_id(&self, account_id: Uuid) -> Result<Option<Vec<u8>>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)