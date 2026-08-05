---
type: Rust Function
title: message_blob_download_hides_bcc_for_delegated_shared_mailbox
resource: crates/lpe-jmap/src/tests.rs#L7327-L7353
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
---

# Signature

`async fn message_blob_download_hides_bcc_for_delegated_shared_mailbox()`

# Calls

- [handle_download](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)