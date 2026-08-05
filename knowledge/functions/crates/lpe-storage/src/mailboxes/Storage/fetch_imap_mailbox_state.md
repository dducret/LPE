---
type: Rust Method
title: fetch_imap_mailbox_state
resource: crates/lpe-storage/src/mailboxes.rs#L225-L253
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`pub async fn fetch_imap_mailbox_state( &self, account_id: Uuid, mailbox_id: Uuid, ) -> Result<ImapMailboxState>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)