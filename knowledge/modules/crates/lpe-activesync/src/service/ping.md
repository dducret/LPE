---
type: Rust Module
title: ping
resource: crates/lpe-activesync/src/service/ping.rs#L1-L371
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-response-response
  - external/lpe-storage-canonicalchangecategory-canonicalchangelistener
  - external/serde-deserialize-serialize
  - external/std-time-duration
  - external/tokio-time-sleep-timeout-instant
  - external/uuid-uuid
  - external/crate-constants-calendar-class-contacts-class-ping-settings-collection-id-response-wbxml-response-snapshot-diff-collection-states-mail-collection-store-activesyncstore-types-authenticatedprincipal-collectiondefinition-storedsyncstate-wbxml-encode-wbxml-wbxmlnode
  - external/super-decode-sync-state-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [PingSettings](../../../../../classes/crates/lpe-activesync/src/service/ping/PingSettings.md)
- [PingFolder](../../../../../classes/crates/lpe-activesync/src/service/ping/PingFolder.md)
- [PingResolution](../../../../../classes/crates/lpe-activesync/src/service/ping/PingResolution.md)
- [handle_ping](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)
- [ping_requires_folder_sync](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync.md)
- [changed_ping_collections](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections.md)
- [wait_for_ping_change](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/wait_for_ping_change.md)
- [ping_status_response](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_status_response.md)
- [ping_settings_from_request](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_settings_from_request.md)
- [load_ping_settings](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/load_ping_settings.md)
- [store_ping_settings](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/store_ping_settings.md)
- [resolve_ping_collections](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/resolve_ping_collections.md)
- [ping_change_categories](../../../../../functions/crates/lpe-activesync/src/service/ping/ping_change_categories.md)
- [ping_deadline](../../../../../functions/crates/lpe-activesync/src/service/ping/ping_deadline.md)
- [ping_heartbeat_duration](../../../../../functions/crates/lpe-activesync/src/service/ping/ping_heartbeat_duration.md)
- [ping_heartbeat_duration](../../../../../functions/crates/lpe-activesync/src/service/ping/ping_heartbeat_duration-2.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::response::Response`
- `lpe_storage::{CanonicalChangeCategory, CanonicalChangeListener}`
- `serde::{Deserialize, Serialize}`
- `std::time::Duration`
- `tokio::time::{sleep, timeout, Instant}`
- `uuid::Uuid`
- `crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, PING_SETTINGS_COLLECTION_ID},
    response::wbxml_response,
    snapshot::{diff_collection_states, mail_collection},
    store::ActiveSyncStore,
    types::{AuthenticatedPrincipal, CollectionDefinition, StoredSyncState},
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{decode_sync_state, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)