---
type: Rust Function
title: email_copy_rejects_inaccessible_source_and_read_only_target
resource: crates/lpe-jmap/src/tests.rs#L7459-L7535
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn email_copy_rejects_inaccessible_source_and_read_only_target()`

# Calls

- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)