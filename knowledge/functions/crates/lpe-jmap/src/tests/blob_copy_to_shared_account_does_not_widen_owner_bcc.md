---
type: Rust Function
title: blob_copy_to_shared_account_does_not_widen_owner_bcc
resource: crates/lpe-jmap/src/tests.rs#L7956-L8002
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn blob_copy_to_shared_account_does_not_widen_owner_bcc()`

# Calls

- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)