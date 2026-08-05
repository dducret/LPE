---
type: Rust Method
title: fetch_imap_highest_modseq
resource: crates/lpe-storage/src/mailboxes.rs#L206-L223
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`pub async fn fetch_imap_highest_modseq(&self, account_id: Uuid) -> Result<u64>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)