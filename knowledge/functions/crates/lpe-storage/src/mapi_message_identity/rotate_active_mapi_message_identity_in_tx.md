---
type: Rust Function
title: rotate_active_mapi_message_identity_in_tx
resource: crates/lpe-storage/src/mapi_message_identity.rs#L16-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
  - functions/crates/lpe-storage/src/mail_items/update_imap_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
---

# Signature

`pub(crate) async fn rotate_active_mapi_message_identity_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [mapi_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)

# Called by

- [add_message_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
- [update_imap_flags](../../../../../functions/crates/lpe-storage/src/mail_items/update_imap_flags.md)
- [update_jmap_email_followup_flags](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [update_jmap_email_content](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [replace_message_recipients](../../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)
- [save_draft_message](../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)