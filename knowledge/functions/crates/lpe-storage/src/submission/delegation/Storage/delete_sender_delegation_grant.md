---
type: Rust Method
title: delete_sender_delegation_grant
resource: crates/lpe-storage/src/submission/delegation.rs#L449-L510
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change
---

# Signature

`pub async fn delete_sender_delegation_grant( &self, owner_account_id: Uuid, grantee_account_id: Uuid, sender_right: SenderDelegationRight, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [allocate_mail_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_delegation_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change.md)