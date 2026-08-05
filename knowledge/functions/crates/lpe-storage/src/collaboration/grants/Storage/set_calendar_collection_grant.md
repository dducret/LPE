---
type: Rust Method
title: set_calendar_collection_grant
resource: crates/lpe-storage/src/collaboration/grants.rs#L439-L571
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change
---

# Signature

`pub async fn set_calendar_collection_grant( &self, owner_account_id: Uuid, calendar_collection_id: &str, grantee_account_id: Uuid, may_read: bool, may_write: bool, may_delete: bool, may_share: bool, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [validate_collaboration_rights](../../../../../../../functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights.md)
- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [load_account_identity_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_collaboration_grant_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change.md)