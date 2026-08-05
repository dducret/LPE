---
type: Rust Module
title: folders
resource: crates/lpe-activesync/src/service/folders.rs#L1-L542
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-response-response
  - external/lpe-domain-mailboxnamepolicy
  - external/lpe-storage-auditentryinput-jmapmailboxcreateinput-jmapmailboxupdateinput
  - external/serde-json-json-value
  - external/uuid-uuid
  - external/crate-constants-folder-sync-collection-id-root-folder-id-protocol-activesyncfoldertype-activesyncstatus-response-wbxml-response-snapshot-diff-snapshots-mail-collection-snapshot-to-value-store-activesyncstore-types-authenticatedprincipal-collectiondefinition-snapshotentry-wbxml-encode-wbxml-wbxmlnode
  - external/super-command-status-response-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_folder_sync](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [handle_folder_create](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [store_current_folder_hierarchy](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy.md)
- [folder_hierarchy_snapshot](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_hierarchy_snapshot.md)
- [push_folder_metadata](../../../../../functions/crates/lpe-activesync/src/service/folders/push_folder_metadata.md)
- [folder_mutation_response](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_mutation_response.md)
- [active_sync_audit](../../../../../functions/crates/lpe-activesync/src/service/folders/active_sync_audit.md)
- [folder_create_error_status](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_create_error_status.md)
- [folder_delete_error_status](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_delete_error_status.md)
- [folder_update_error_status](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_update_error_status.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::response::Response`
- `lpe_domain::MailboxNamePolicy`
- `lpe_storage::{AuditEntryInput, JmapMailboxCreateInput, JmapMailboxUpdateInput}`
- `serde_json::{json, Value}`
- `uuid::Uuid`
- `crate::{
    constants::{FOLDER_SYNC_COLLECTION_ID, ROOT_FOLDER_ID},
    protocol::{ActiveSyncFolderType, ActiveSyncStatus},
    response::wbxml_response,
    snapshot::{diff_snapshots, mail_collection, snapshot_to_value},
    store::ActiveSyncStore,
    types::{AuthenticatedPrincipal, CollectionDefinition, SnapshotEntry},
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{command_status_response, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)