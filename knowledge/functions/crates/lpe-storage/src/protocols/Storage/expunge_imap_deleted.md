---
type: Rust Method
title: expunge_imap_deleted
resource: crates/lpe-storage/src/protocols.rs#L1155-L1164
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub async fn expunge_imap_deleted( &self, account_id: Uuid, mailbox_id: Uuid, message_ids: &[Uuid], audit: AuditEntryInput, ) -> Result<()>`