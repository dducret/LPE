---
type: Rust Function
title: email_changes_report_updates_for_existing_messages
resource: crates/lpe-jmap/src/tests.rs#L4222-L4274
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
---

# Signature

`async fn email_changes_report_updates_for_existing_messages()`

# Calls

- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)