---
type: Rust Function
title: jmap_mail_query_snippet_and_blob_projections_do_not_expose_bcc
resource: crates/lpe-jmap/src/tests.rs#L3864-L3930
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn jmap_mail_query_snippet_and_blob_projections_do_not_expose_bcc()`

# Calls

- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)