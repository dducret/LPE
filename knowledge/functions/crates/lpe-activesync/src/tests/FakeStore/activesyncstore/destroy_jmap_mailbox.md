---
type: Rust Method
title: destroy_jmap_mailbox
resource: crates/lpe-activesync/src/tests.rs#L421-L455
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes
  - functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes
---

# Signature

`fn destroy_jmap_mailbox<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, _audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes.md)
- [set_current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes.md)