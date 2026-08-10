---
type: Rust Method
title: copy_jmap_email_between_accounts
resource: crates/lpe-jmap/src/tests.rs#L1469-L1523
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn copy_jmap_email_between_accounts( &self, _source_account_id: Uuid, target_account_id: Uuid, _message_id: Uuid, target_mailbox_id: Uuid, _audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [draft_email](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)