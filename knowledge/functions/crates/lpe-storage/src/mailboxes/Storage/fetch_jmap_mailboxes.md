---
type: Rust Method
title: fetch_jmap_mailboxes
resource: crates/lpe-storage/src/mailboxes.rs#L56-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`pub async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)