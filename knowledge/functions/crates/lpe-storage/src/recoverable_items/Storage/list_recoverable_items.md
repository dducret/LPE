---
type: Rust Method
title: list_recoverable_items
resource: crates/lpe-storage/src/recoverable_items.rs#L29-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn list_recoverable_items( &self, account_id: Uuid, recoverable_folder: Option<&str>, ) -> Result<Vec<RecoverableItem>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)