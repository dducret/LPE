---
type: Rust Method
title: upsert_sender_delegation_grant
resource: crates/lpe-storage/src/submission/delegation.rs#L375-L447
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_delegation_grant
---

# Signature

`pub async fn upsert_sender_delegation_grant( &self, input: SenderDelegationGrantInput, audit: AuditEntryInput, ) -> Result<SenderDelegationGrant>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [load_account_identity_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [load_account_identity_by_email_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx.md)
- [allocate_mail_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_delegation_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change.md)
- [fetch_sender_delegation_grant](../../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_delegation_grant.md)