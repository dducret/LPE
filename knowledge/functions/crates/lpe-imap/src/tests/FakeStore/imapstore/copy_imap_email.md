---
type: Rust Method
title: copy_imap_email
resource: crates/lpe-imap/src/tests.rs#L586-L640
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/FakeStore/next_modseq
  - functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn copy_imap_email<'a>( &'a self, _account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, _audit: AuditEntryInput, ) -> StoreFuture<'a, ImapEmail>`

# Calls

- [next_modseq](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/next_modseq.md)
- [allocate_uid](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)