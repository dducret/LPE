---
type: Rust Method
title: create_canonical_change_listener
resource: crates/lpe-storage/src/change.rs#L176-L188
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn create_canonical_change_listener( &self, principal_account_id: Uuid, ) -> Result<CanonicalChangeListener>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)