---
type: Rust Function
title: identity_shared_account_reads_reject_inaccessible_accounts
resource: crates/lpe-jmap/src/tests.rs#L5855-L5900
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn identity_shared_account_reads_reject_inaccessible_accounts()`

# Calls

- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)