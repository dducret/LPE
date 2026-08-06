---
type: Rust Method
title: create_accessible_contact
resource: crates/lpe-jmap/src/tests.rs#L1654-L1690
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn create_accessible_contact( &self, _principal_account_id: Uuid, _collection_id: Option<&str>, input: UpsertClientContactInput, ) -> Result<AccessibleContact>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)