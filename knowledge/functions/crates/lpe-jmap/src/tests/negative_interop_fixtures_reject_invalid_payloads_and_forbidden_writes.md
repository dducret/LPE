---
type: Rust Function
title: negative_interop_fixtures_reject_invalid_payloads_and_forbidden_writes
resource: crates/lpe-jmap/src/tests.rs#L13827-L13926
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn negative_interop_fixtures_reject_invalid_payloads_and_forbidden_writes()`

# Calls

- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)