---
type: Rust Method
title: fetch_jmap_message_blob
resource: crates/lpe-activesync/src/tests.rs#L633-L653
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_jmap_message_blob<'a>( &'a self, account_id: Uuid, message_id: Uuid, ) -> StoreFuture<'a, Option<JmapUploadBlob>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)