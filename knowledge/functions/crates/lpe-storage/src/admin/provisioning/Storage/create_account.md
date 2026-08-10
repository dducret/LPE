---
type: Rust Method
title: create_account
resource: crates/lpe-storage/src/admin/provisioning.rs#L14-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/util/normalize_gal_visibility
  - functions/crates/lpe-storage/src/util/normalize_directory_kind
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn create_account(&self, input: NewAccount, audit: AuditEntryInput) -> Result<()>`

# Calls

- [tenant_id_for_account_email](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [normalize_gal_visibility](../../../../../../../functions/crates/lpe-storage/src/util/normalize_gal_visibility.md)
- [normalize_directory_kind](../../../../../../../functions/crates/lpe-storage/src/util/normalize_directory_kind.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)