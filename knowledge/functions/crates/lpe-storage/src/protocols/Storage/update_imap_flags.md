---
type: Rust Method
title: update_imap_flags
resource: crates/lpe-storage/src/protocols.rs#L1132-L1153
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub async fn update_imap_flags( &self, account_id: Uuid, mailbox_id: Uuid, message_ids: &[Uuid], unread: Option<bool>, flagged: Option<bool>, deleted: Option<bool>, unchanged_since: Option<u64>, ) -> Result<Vec<Uuid>>`