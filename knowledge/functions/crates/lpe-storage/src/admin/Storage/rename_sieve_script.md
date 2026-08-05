---
type: Rust Method
title: rename_sieve_script
resource: crates/lpe-storage/src/admin.rs#L561-L636
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/util/validate_sieve_script_name
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn rename_sieve_script( &self, account_id: Uuid, old_name: &str, new_name: &str, audit: AuditEntryInput, ) -> Result<SieveScriptSummary>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_sieve_script_name](../../../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_name.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)