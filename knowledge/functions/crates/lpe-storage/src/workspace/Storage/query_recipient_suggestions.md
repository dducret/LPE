---
type: Rust Method
title: query_recipient_suggestions
resource: crates/lpe-storage/src/workspace.rs#L909-L958
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn query_recipient_suggestions( &self, account_id: Uuid, query: Option<&str>, ) -> Result<Vec<RecipientSuggestion>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)