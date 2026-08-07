---
type: Rust Method
title: delete_message_attachment
resource: crates/lpe-storage/src/attachments.rs#L912-L1022
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/parse_activesync_file_reference
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn delete_message_attachment( &self, account_id: Uuid, file_reference: &str, audit: AuditEntryInput, ) -> Result<Option<JmapEmail>>`

# Calls

- [parse_activesync_file_reference](../../../../../../functions/crates/lpe-storage/src/util/parse_activesync_file_reference.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)