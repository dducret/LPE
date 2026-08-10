---
type: Rust Function
title: session_omits_submission_for_shared_mailbox_without_sender_grant
resource: crates/lpe-jmap/src/tests.rs#L2703-L2730
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
---

# Signature

`async fn session_omits_submission_for_shared_mailbox_without_sender_grant()`

# Calls

- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)