---
type: Rust Method
title: fetch_imap_emails
resource: crates/lpe-storage/src/imap.rs#L61-L272
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`pub async fn fetch_imap_emails( &self, account_id: Uuid, mailbox_id: Uuid, ) -> Result<Vec<ImapEmail>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)