---
type: Rust Method
title: delete_jmap_email_from_mailbox
resource: crates/lpe-storage/src/protocols.rs#L1183-L1194
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub async fn delete_jmap_email_from_mailbox( &self, account_id: Uuid, mailbox_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`