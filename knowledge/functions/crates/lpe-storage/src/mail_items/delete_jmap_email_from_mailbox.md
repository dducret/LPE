---
type: Rust Function
title: delete_jmap_email_from_mailbox
resource: crates/lpe-storage/src/mail_items.rs#L493-L501
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
---

# Signature

`pub async fn delete_jmap_email_from_mailbox( storage: &Storage, account_id: Uuid, mailbox_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [delete_jmap_email_memberships](../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)