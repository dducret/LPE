---
type: Rust Method
title: cancel_queued_submission
resource: crates/lpe-exchange/src/tests/mod.rs#L11884-L11914
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn cancel_queued_submission<'a>( &'a self, _account_id: Uuid, message_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, CancelSubmissionResult>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)