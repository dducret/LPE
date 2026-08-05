---
type: Rust Method
title: submit_message
resource: crates/lpe-exchange/src/tests/mod.rs#L11794-L11882
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses
---

# Signature

`fn submit_message<'a>( &'a self, input: SubmitMessageInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, SubmittedMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [email_addresses](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses.md)