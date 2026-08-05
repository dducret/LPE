---
type: Rust Method
title: ensure_account_exists
resource: crates/lpe-storage/src/shared.rs#L87-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action
  - functions/crates/lpe-storage/src/jmap_blobs/Storage/save_jmap_upload_blob
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder
  - functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
---

# Signature

`pub(crate) async fn ensure_account_exists( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [put_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [upsert_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action.md)
- [save_jmap_upload_blob](../../../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/save_jmap_upload_blob.md)
- [ensure_imap_mailboxes](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [upsert_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder.md)
- [replace_message_recipients](../../../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)