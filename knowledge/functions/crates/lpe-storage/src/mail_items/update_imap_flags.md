---
type: Rust Function
title: update_imap_flags
resource: crates/lpe-storage/src/mail_items.rs#L83-L297
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn update_imap_flags( storage: &Storage, account_id: Uuid, mailbox_id: Uuid, message_ids: &[Uuid], unread: Option<bool>, flagged: Option<bool>, deleted: Option<bool>, unchanged_since: Option<u64>, ) -> Result<Vec<Uuid>>`

# Calls

- [tenant_id_for_account_id](../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [allocate_mail_modseq_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [affected_mail_principals_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [recalculate_mailbox_counts_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)
- [emit_mail_change](../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)