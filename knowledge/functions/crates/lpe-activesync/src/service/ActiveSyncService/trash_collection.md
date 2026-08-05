---
type: Rust Method
title: trash_collection
resource: crates/lpe-activesync/src/service.rs#L1034-L1043
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
---

# Signature

`async fn trash_collection(&self, account_id: Uuid) -> Result<Option<CollectionDefinition>>`

# Calls

- [folder_collections](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)

# Called by

- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)