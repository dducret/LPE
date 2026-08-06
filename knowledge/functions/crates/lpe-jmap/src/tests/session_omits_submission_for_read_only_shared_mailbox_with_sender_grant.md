---
type: Rust Function
title: session_omits_submission_for_read_only_shared_mailbox_with_sender_grant
resource: crates/lpe-jmap/src/tests.rs#L2788-L2852
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn session_omits_submission_for_read_only_shared_mailbox_with_sender_grant()`

# Calls

- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)