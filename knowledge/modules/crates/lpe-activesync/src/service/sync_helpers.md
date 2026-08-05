---
type: Rust Module
title: sync_helpers
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L1-L137
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/serde-json-value
  - external/crate-constants-calendar-class-contacts-class-root-folder-id-snapshot-drafts-collection-mail-collection-types-collectiondefinition-collectionstateentry-snapshotchange-storedsyncstate-wbxml-wbxmlnode
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [decode_sync_state](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state.md)
- [completed_sync_state](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/completed_sync_state.md)
- [has_client_commands](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/has_client_commands.md)
- [sync_collection_status_node](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_status_node.md)
- [sync_collection_has_unsupported_command](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_has_unsupported_command.md)
- [sync_command_supported_for_collection](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection.md)
- [pending_page](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/pending_page.md)
- [value_to_wbxml](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml.md)
- [hierarchy_generation](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation.md)
- [hierarchy_generation_from_snapshot](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation_from_snapshot.md)

# Imports

- `anyhow::Result`
- `serde_json::Value`
- `crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, ROOT_FOLDER_ID},
    snapshot::{drafts_collection, mail_collection},
    types::{CollectionDefinition, CollectionStateEntry, SnapshotChange, StoredSyncState},
    wbxml::WbxmlNode,
}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)