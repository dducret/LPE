---
type: Rust Method
title: fetch_activesync_message_attachments
resource: crates/lpe-activesync/src/tests.rs#L803-L816
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_activesync_message_attachments<'a>( &'a self, _account_id: Uuid, message_id: Uuid, ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)