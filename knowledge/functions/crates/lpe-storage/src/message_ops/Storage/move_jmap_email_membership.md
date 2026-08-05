---
type: Rust Method
title: move_jmap_email_membership
resource: crates/lpe-storage/src/message_ops.rs#L261-L710
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
  called_by:
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox_with_mapi_identity
---

# Signature

`async fn move_jmap_email_membership( &self, account_id: Uuid, source_mailbox_id: Option<Uuid>, message_id: Uuid, target_mailbox_id: Uuid, imported_identity: Option<&MapiMessageImportedMoveIdentity>, audit: AuditEntryInput, ) -> Result<(JmapEmail, Option<MapiMessageIdentityMove>)>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [rekey_mapi_message_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)
- [recalculate_mailbox_counts_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)

# Called by

- [move_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email.md)
- [move_jmap_email_from_mailbox](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox.md)
- [move_jmap_email_from_mailbox_with_mapi_identity](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox_with_mapi_identity.md)