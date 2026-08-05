---
type: Rust Method
title: restore_recoverable_item
resource: crates/lpe-admin-api/src/workspace/tests.rs#L390-L404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-admin-api/src/workspace/tests/jmap_email
---

# Signature

`async fn restore_recoverable_item( &self, account_id: Uuid, recoverable_item_id: Uuid, target_mailbox_id: Option<Uuid>, audit: AuditEntryInput, ) -> anyhow::Result<JmapEmail>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [jmap_email](../../../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/jmap_email.md)