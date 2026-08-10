---
type: Rust Method
title: copy_jmap_email_between_accounts
resource: crates/lpe-storage/src/message_ops.rs#L105-L218
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn copy_jmap_email_between_accounts( &self, source_account_id: Uuid, target_account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)