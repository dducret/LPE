---
type: Rust Method
title: save_draft_message
resource: crates/lpe-exchange/src/tests/mod.rs#L11756-L11797
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn save_draft_message<'a>( &'a self, input: SubmitMessageInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, SavedDraftMessage>`

# Calls

- [email_addresses](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)