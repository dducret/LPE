---
type: Rust Method
title: move_jmap_email_from_mailbox
resource: crates/lpe-storage/src/message_ops.rs#L217-L235
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
---

# Signature

`pub async fn move_jmap_email_from_mailbox( &self, account_id: Uuid, source_mailbox_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)