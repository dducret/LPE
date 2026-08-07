---
type: Rust Method
title: restore_recoverable_item
resource: crates/lpe-admin-api/src/workspace.rs#L273-L288
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn restore_recoverable_item( &self, account_id: Uuid, recoverable_item_id: Uuid, target_mailbox_id: Option<Uuid>, audit: AuditEntryInput, ) -> anyhow::Result<JmapEmail>`