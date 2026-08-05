---
type: Rust Method
title: ensure_imap_mailboxes
resource: crates/lpe-storage/src/mailboxes.rs#L117-L204
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders
  - functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn ensure_imap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [ensure_exchange_search_folders](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders.md)
- [ensure_mailbox](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)