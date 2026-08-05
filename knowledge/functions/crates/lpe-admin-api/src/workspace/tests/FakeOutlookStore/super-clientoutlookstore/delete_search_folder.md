---
type: Rust Method
title: delete_search_folder
resource: crates/lpe-admin-api/src/workspace/tests.rs#L340-L354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn delete_search_folder( &self, account_id: Uuid, search_folder_id: Uuid, ) -> anyhow::Result<()>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)