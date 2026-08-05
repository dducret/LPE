---
type: Rust Method
title: purge_recoverable_item
resource: crates/lpe-admin-api/src/workspace/tests.rs#L406-L418
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn purge_recoverable_item( &self, account_id: Uuid, recoverable_item_id: Uuid, audit: AuditEntryInput, ) -> anyhow::Result<()>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)