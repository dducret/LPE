---
type: Rust Function
title: blob_create_paths_reject_read_only_shared_accounts
resource: crates/lpe-jmap/src/tests.rs#L8120-L8213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/validator_ok
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
---

# Signature

`async fn blob_create_paths_reject_read_only_shared_accounts()`

# Calls

- [validator_ok](../../../../../functions/crates/lpe-jmap/src/tests/validator_ok.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [handle_upload](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)