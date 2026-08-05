---
type: Rust Method
title: submit_draft_message
resource: crates/lpe-jmap/src/tests.rs#L1382-L1408
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
---

# Signature

`async fn submit_draft_message( &self, account_id: Uuid, draft_message_id: Uuid, submitted_by_account_id: Uuid, source: &str, _audit: AuditEntryInput, ) -> Result<SubmittedMessage>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [draft_email](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)