---
type: Rust Method
title: update_account
resource: crates/lpe-storage/src/admin/provisioning.rs#L113-L177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/util/normalize_gal_visibility
  - functions/crates/lpe-storage/src/util/normalize_directory_kind
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn update_account(&self, input: UpdateAccount, audit: AuditEntryInput) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [normalize_gal_visibility](../../../../../../../functions/crates/lpe-storage/src/util/normalize_gal_visibility.md)
- [normalize_directory_kind](../../../../../../../functions/crates/lpe-storage/src/util/normalize_directory_kind.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)