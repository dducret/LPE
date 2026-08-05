---
type: Rust Method
title: fetch_canonical_change_cursor
resource: crates/lpe-storage/src/change.rs#L573-L589
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_canonical_change_cursor( &self, principal_account_id: Uuid, ) -> Result<Option<i64>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)