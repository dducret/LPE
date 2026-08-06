---
type: Rust Method
title: destroy_jmap_mailbox
resource: crates/lpe-jmap/src/tests.rs#L1135-L1145
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/mailbox_is_system
---

# Signature

`async fn destroy_jmap_mailbox( &self, _account_id: Uuid, mailbox_id: Uuid, _audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [mailbox_is_system](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/mailbox_is_system.md)