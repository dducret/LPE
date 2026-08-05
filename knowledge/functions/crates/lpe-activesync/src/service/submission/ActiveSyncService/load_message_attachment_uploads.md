---
type: Rust Method
title: load_message_attachment_uploads
resource: crates/lpe-activesync/src/service/submission.rs#L261-L288
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`async fn load_message_attachment_uploads( &self, account_id: Uuid, message_id: Uuid, ) -> Result<Vec<lpe_storage::AttachmentUploadInput>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_smart_compose](../../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)