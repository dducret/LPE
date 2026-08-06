---
type: Rust Method
title: move_jmap_email
resource: crates/lpe-storage/src/message_ops.rs#L220-L237
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
---

# Signature

`pub async fn move_jmap_email( &self, account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)