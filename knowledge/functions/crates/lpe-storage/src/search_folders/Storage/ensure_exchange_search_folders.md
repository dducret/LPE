---
type: Rust Method
title: ensure_exchange_search_folders
resource: crates/lpe-storage/src/search_folders.rs#L451-L538
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/search_folders/exchange_builtin_search_folder_definitions
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes
---

# Signature

`pub(crate) async fn ensure_exchange_search_folders( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<()>`

# Calls

- [exchange_builtin_search_folder_definitions](../../../../../../functions/crates/lpe-storage/src/search_folders/exchange_builtin_search_folder_definitions.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)

# Called by

- [ensure_imap_mailboxes](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes.md)