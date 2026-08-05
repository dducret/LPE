---
type: Rust Method
title: fetch_jmap_mail_change_cursor
resource: crates/lpe-storage/src/protocols.rs#L166-L183
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_jmap_mail_change_cursor(&self, account_id: Uuid) -> Result<Option<i64>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)