---
type: Rust Function
title: email_copy_allows_shared_source_to_owned_target_without_exposing_bcc
resource: crates/lpe-jmap/src/tests.rs#L7376-L7453
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn email_copy_allows_shared_source_to_owned_target_without_exposing_bcc()`

# Calls

- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)