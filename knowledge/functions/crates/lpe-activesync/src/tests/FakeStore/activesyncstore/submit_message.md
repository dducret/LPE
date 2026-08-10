---
type: Rust Method
title: submit_message
resource: crates/lpe-activesync/src/tests.rs#L975-L992
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn submit_message<'a>( &'a self, input: SubmitMessageInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, SubmittedMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)