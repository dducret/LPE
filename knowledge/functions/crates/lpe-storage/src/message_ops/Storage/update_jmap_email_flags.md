---
type: Rust Method
title: update_jmap_email_flags
resource: crates/lpe-storage/src/message_ops.rs#L712-L728
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail_items/update_message_flags
---

# Signature

`pub async fn update_jmap_email_flags( &self, account_id: Uuid, message_id: Uuid, unread: Option<bool>, flagged: Option<bool>, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [update_message_flags](../../../../../../functions/crates/lpe-storage/src/mail_items/update_message_flags.md)