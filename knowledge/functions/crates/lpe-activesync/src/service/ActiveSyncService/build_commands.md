---
type: Rust Method
title: build_commands
resource: crates/lpe-activesync/src/service.rs#L704-L743
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn build_commands( &self, account_id: Uuid, collection: &CollectionDefinition, page_changes: &[SnapshotChange], body_preference: &BodyPreference, ) -> Result<WbxmlNode>`

# Calls

- [fetch_collection_nodes](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)