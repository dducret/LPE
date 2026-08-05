---
type: Rust Method
title: apply_conversation_actions_to_jmap_email
resource: crates/lpe-storage/src/conversation_actions.rs#L302-L370
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`pub async fn apply_conversation_actions_to_jmap_email( &self, account_id: Uuid, message_id: Uuid, actor: &str, ) -> Result<()>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)