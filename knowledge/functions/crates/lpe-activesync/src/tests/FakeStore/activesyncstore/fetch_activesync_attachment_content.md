---
type: Rust Method
title: fetch_activesync_attachment_content
resource: crates/lpe-activesync/src/tests.rs#L818-L830
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_activesync_attachment_content<'a>( &'a self, _account_id: Uuid, file_reference: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)