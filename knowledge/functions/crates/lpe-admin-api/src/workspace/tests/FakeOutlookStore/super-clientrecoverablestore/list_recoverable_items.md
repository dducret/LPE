---
type: Rust Method
title: list_recoverable_items
resource: crates/lpe-admin-api/src/workspace/tests.rs#L366-L388
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn list_recoverable_items( &self, account_id: Uuid, recoverable_folder: Option<&str>, ) -> anyhow::Result<Vec<RecoverableItem>>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)