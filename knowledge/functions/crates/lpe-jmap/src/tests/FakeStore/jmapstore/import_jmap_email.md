---
type: Rust Method
title: import_jmap_email
resource: crates/lpe-jmap/src/tests.rs#L1448-L1535
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn import_jmap_email( &self, input: JmapImportedEmailInput, _audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)