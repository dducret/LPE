---
type: Rust Method
title: create_jmap_mailbox
resource: crates/lpe-activesync/src/tests.rs#L328-L367
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes
---

# Signature

`fn create_jmap_mailbox<'a>( &'a self, input: JmapMailboxCreateInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [set_current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes.md)