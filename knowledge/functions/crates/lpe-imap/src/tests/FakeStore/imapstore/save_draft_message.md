---
type: Rust Method
title: save_draft_message
resource: crates/lpe-imap/src/tests.rs#L678-L754
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

`fn save_draft_message<'a>( &'a self, input: SubmitMessageInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, SavedDraftMessage>`

# Calls

- [allocate_uid](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid.md)
- [next_modseq](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/next_modseq.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)