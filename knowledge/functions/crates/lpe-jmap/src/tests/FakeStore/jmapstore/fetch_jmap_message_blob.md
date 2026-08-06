---
type: Rust Method
title: fetch_jmap_message_blob
resource: crates/lpe-jmap/src/tests.rs#L1378-L1396
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/tests/strip_bcc_headers_for_test
---

# Signature

`async fn fetch_jmap_message_blob( &self, _account_id: Uuid, message_id: Uuid, ) -> Result<Option<JmapUploadBlob>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [strip_bcc_headers_for_test](../../../../../../../functions/crates/lpe-jmap/src/tests/strip_bcc_headers_for_test.md)