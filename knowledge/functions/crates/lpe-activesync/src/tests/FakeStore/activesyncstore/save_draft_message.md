---
type: Rust Method
title: save_draft_message
resource: crates/lpe-activesync/src/tests.rs#L946-L963
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn save_draft_message<'a>( &'a self, input: SubmitMessageInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, SavedDraftMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)