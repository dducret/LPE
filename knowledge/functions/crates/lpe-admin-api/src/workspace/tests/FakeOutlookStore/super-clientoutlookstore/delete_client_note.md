---
type: Rust Method
title: delete_client_note
resource: crates/lpe-admin-api/src/workspace/tests.rs#L205-L212
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn delete_client_note(&self, account_id: Uuid, note_id: Uuid) -> anyhow::Result<()>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)