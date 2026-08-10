---
type: Rust Function
title: insert_visible_recipient
resource: crates/lpe-storage/src/submission.rs#L32-L58
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients
---

# Signature

`async fn insert_visible_recipient( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, message_id: Uuid, role: &str, ordinal: usize, recipient: &SubmittedRecipientInput, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [replace_message_recipients](../../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)