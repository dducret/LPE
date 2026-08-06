---
type: Rust Method
title: copy_jmap_email
resource: crates/lpe-storage/src/message_ops.rs#L88-L103
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub async fn copy_jmap_email( &self, account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<JmapEmail>`