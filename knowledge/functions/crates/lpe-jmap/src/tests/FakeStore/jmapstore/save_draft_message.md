---
type: Rust Method
title: save_draft_message
resource: crates/lpe-jmap/src/tests.rs#L1398-L1411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn save_draft_message( &self, input: SubmitMessageInput, _audit: AuditEntryInput, ) -> Result<SavedDraftMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)