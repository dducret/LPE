---
type: Rust Method
title: upsert_jmap_share
resource: crates/lpe-jmap/src/tests.rs#L2332-L2370
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn upsert_jmap_share( &self, input: crate::store::JmapShareInput, _audit: AuditEntryInput, ) -> Result<Value>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)