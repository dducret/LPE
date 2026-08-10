---
type: Rust Method
title: update_jmap_mailbox
resource: crates/lpe-jmap/src/tests.rs#L1114-L1135
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/mailbox_is_system
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn update_jmap_mailbox( &self, input: JmapMailboxUpdateInput, _audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [mailbox_is_system](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/mailbox_is_system.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)