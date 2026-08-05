---
type: Rust Method
title: copy_jmap_email
resource: crates/lpe-jmap/src/tests.rs#L1410-L1446
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
---

# Signature

`async fn copy_jmap_email( &self, _account_id: Uuid, _message_id: Uuid, target_mailbox_id: Uuid, _audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [draft_email](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)