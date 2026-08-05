---
type: Rust Method
title: list_mailbox_rules
resource: crates/lpe-storage/src/admin.rs#L158-L197
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries
  - functions/crates/lpe-storage/src/admin/helpers/unsupported_exchange_rule_features
---

# Signature

`pub async fn list_mailbox_rules(&self, account_id: Uuid) -> Result<Vec<MailboxRule>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mailbox_rule_summaries](../../../../../../functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries.md)
- [unsupported_exchange_rule_features](../../../../../../functions/crates/lpe-storage/src/admin/helpers/unsupported_exchange_rule_features.md)