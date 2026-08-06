---
type: Rust Method
title: copy_jmap_email_between_accounts
resource: crates/lpe-jmap/src/store.rs#L889-L905
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn copy_jmap_email_between_accounts( &self, source_account_id: Uuid, target_account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<JmapEmail>`