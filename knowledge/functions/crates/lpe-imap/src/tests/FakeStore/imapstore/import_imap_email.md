---
type: Rust Method
title: import_imap_email
resource: crates/lpe-imap/src/tests.rs#L756-L824
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid
  - functions/crates/lpe-imap/src/tests/FakeStore/next_modseq
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn import_imap_email<'a>( &'a self, input: JmapImportedEmailInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, ImapEmail>`

# Calls

- [allocate_uid](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid.md)
- [next_modseq](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/next_modseq.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)