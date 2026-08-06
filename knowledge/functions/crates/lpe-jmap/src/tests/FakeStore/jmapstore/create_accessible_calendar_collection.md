---
type: Rust Method
title: create_accessible_calendar_collection
resource: crates/lpe-jmap/src/tests.rs#L1757-L1770
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn create_accessible_calendar_collection( &self, _principal_account_id: Uuid, display_name: &str, ) -> Result<CollaborationCollection>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)