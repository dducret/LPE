---
type: Rust Method
title: fetch_conversation_actions_by_ids
resource: crates/lpe-storage/src/conversation_actions.rs#L94-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_conversation_actions_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ConversationAction>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)