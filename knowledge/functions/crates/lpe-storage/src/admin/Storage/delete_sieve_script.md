---
type: Rust Method
title: delete_sieve_script
resource: crates/lpe-storage/src/admin.rs#L493-L559
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/util/validate_sieve_script_name
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn delete_sieve_script( &self, account_id: Uuid, name: &str, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_sieve_script_name](../../../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_name.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_collaboration_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)