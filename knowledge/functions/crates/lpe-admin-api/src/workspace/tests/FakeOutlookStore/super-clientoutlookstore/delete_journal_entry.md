---
type: Rust Method
title: delete_journal_entry
resource: crates/lpe-admin-api/src/workspace/tests.rs#L263-L273
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn delete_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> anyhow::Result<()>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)