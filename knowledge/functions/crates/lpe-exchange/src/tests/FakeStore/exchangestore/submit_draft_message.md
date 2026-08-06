---
type: Rust Method
title: submit_draft_message
resource: crates/lpe-exchange/src/tests/mod.rs#L11873-L11939
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn submit_draft_message<'a>( &'a self, account_id: Uuid, draft_message_id: Uuid, submitted_by_account_id: Uuid, source: &'a str, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, SubmittedMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)