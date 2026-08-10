---
type: Rust Method
title: create_accessible_event
resource: crates/lpe-jmap/src/tests.rs#L1834-L1870
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn create_accessible_event( &self, _principal_account_id: Uuid, _collection_id: Option<&str>, input: UpsertClientEventInput, ) -> Result<AccessibleEvent>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)