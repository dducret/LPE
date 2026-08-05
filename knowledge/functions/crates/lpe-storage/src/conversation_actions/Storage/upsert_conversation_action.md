---
type: Rust Method
title: upsert_conversation_action
resource: crates/lpe-storage/src/conversation_actions.rs#L133-L244
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/conversation_actions/map_conversation_action
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
---

# Signature

`pub async fn upsert_conversation_action( &self, input: UpsertConversationActionInput, ) -> Result<ConversationAction>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [map_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/map_conversation_action.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)