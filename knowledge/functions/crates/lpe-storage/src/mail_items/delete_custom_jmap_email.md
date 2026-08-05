---
type: Rust Function
title: delete_custom_jmap_email
resource: crates/lpe-storage/src/mail_items.rs#L443-L482
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn delete_custom_jmap_email( storage: &Storage, account_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)