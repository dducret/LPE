---
type: Rust Method
title: add_message_attachment
resource: crates/lpe-exchange/src/tests/mod.rs#L11344-L11392
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/state/entry
---

# Signature

`fn add_message_attachment<'a>( &'a self, _account_id: Uuid, message_id: Uuid, attachment: AttachmentUploadInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)