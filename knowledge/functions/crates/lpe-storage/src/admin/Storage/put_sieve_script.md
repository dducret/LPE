---
type: Rust Method
title: put_sieve_script
resource: crates/lpe-storage/src/admin.rs#L355-L491
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/util/validate_sieve_script_name
  - functions/crates/lpe-storage/src/util/validate_sieve_script_content
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn put_sieve_script( &self, account_id: Uuid, name: &str, content: &str, activate: bool, audit: AuditEntryInput, ) -> Result<SieveScriptDocument>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_sieve_script_name](../../../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_name.md)
- [validate_sieve_script_content](../../../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_content.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)