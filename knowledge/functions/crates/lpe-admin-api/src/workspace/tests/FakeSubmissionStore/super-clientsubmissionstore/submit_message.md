---
type: Rust Method
title: submit_message
resource: crates/lpe-admin-api/src/workspace/tests.rs#L112-L128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn submit_message( &self, input: SubmitMessageInput, audit: AuditEntryInput, ) -> anyhow::Result<SubmittedMessage>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)