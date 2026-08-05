---
type: Rust Method
title: fetch_conversation_actions
resource: crates/lpe-storage/src/conversation_actions.rs#L60-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_conversation_actions( &self, account_id: Uuid, ) -> Result<Vec<ConversationAction>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)