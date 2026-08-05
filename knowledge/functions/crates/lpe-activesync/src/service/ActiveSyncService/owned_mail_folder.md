---
type: Rust Method
title: owned_mail_folder
resource: crates/lpe-activesync/src/service.rs#L1431-L1442
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
---

# Signature

`async fn owned_mail_folder( &self, account_id: Uuid, collection_id: &str, ) -> Result<Option<CollectionDefinition>>`

# Calls

- [resolve_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)

# Called by

- [handle_folder_delete](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)