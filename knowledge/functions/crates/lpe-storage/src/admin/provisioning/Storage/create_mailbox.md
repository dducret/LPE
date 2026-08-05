---
type: Rust Method
title: create_mailbox
resource: crates/lpe-storage/src/admin/provisioning.rs#L179-L234
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn create_mailbox(&self, input: NewMailbox, audit: AuditEntryInput) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_mailbox_name_available_in_tx](../../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)