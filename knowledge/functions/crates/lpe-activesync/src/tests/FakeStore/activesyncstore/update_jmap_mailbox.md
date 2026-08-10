---
type: Rust Method
title: update_jmap_mailbox
resource: crates/lpe-activesync/src/tests.rs#L369-L420
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes
---

# Signature

`fn update_jmap_mailbox<'a>( &'a self, input: JmapMailboxUpdateInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [set_current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes.md)