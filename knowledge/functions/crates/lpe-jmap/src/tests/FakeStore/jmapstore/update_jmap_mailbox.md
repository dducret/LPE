---
type: Rust Method
title: update_jmap_mailbox
resource: crates/lpe-jmap/src/tests.rs#L1099-L1117
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn update_jmap_mailbox( &self, input: JmapMailboxUpdateInput, _audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)