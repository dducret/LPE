---
type: Rust Function
title: owned_message_download_prefers_sanitized_stored_raw_mime_blob
resource: crates/lpe-jmap/src/tests.rs#L7280-L7324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/tests/validator_ok
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download
---

# Signature

`async fn owned_message_download_prefers_sanitized_stored_raw_mime_blob()`

# Calls

- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [validator_ok](../../../../../functions/crates/lpe-jmap/src/tests/validator_ok.md)
- [handle_download](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download.md)