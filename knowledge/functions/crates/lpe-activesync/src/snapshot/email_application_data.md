---
type: Rust Function
title: email_application_data
resource: crates/lpe-activesync/src/snapshot.rs#L35-L94
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/snapshot/email_flag_value
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
---

# Signature

`pub(crate) fn email_application_data( email: &JmapEmail, attachments: &[ActiveSyncAttachment], body_preference: &BodyPreference, mime_blob: Option<&JmapUploadBlob>, ) -> Value`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [email_flag_value](../../../../../functions/crates/lpe-activesync/src/snapshot/email_flag_value.md)

# Called by

- [fetch_collection_nodes](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)
- [handle_item_operations_fetch](../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [handle_search](../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)