---
type: Rust Method
title: create_jmap_mailbox
resource: crates/lpe-jmap/src/tests.rs#L1092-L1110
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn create_jmap_mailbox( &self, input: JmapMailboxCreateInput, _audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)