---
type: Rust Module
title: activesync
resource: crates/lpe-storage/src/activesync.rs#L1-L745
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/serde-serialize
  - external/serde-json-value
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-blob-store-durableblobkind-postgresblobstore-activesyncdevicerow-activesyncsyncstaterow-storage
  - external/super-activesync-collection-kind
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ActiveSyncDeviceState](../../../../classes/crates/lpe-storage/src/activesync/ActiveSyncDeviceState.md)
- [ActiveSyncSyncState](../../../../classes/crates/lpe-storage/src/activesync/ActiveSyncSyncState.md)
- [ActiveSyncItemState](../../../../classes/crates/lpe-storage/src/activesync/ActiveSyncItemState.md)
- [ActiveSyncAttachment](../../../../classes/crates/lpe-storage/src/activesync/ActiveSyncAttachment.md)
- [ActiveSyncAttachmentContent](../../../../classes/crates/lpe-storage/src/activesync/ActiveSyncAttachmentContent.md)
- [store_activesync_sync_state](../../../../functions/crates/lpe-storage/src/activesync/Storage/store_activesync_sync_state.md)
- [fetch_activesync_device](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_device.md)
- [store_activesync_device_pending_policy](../../../../functions/crates/lpe-storage/src/activesync/Storage/store_activesync_device_pending_policy.md)
- [acknowledge_activesync_device_policy](../../../../functions/crates/lpe-storage/src/activesync/Storage/acknowledge_activesync_device_policy.md)
- [touch_activesync_device](../../../../functions/crates/lpe-storage/src/activesync/Storage/touch_activesync_device.md)
- [cleanup_expired_activesync_sync_cursors](../../../../functions/crates/lpe-storage/src/activesync/Storage/cleanup_expired_activesync_sync_cursors.md)
- [fetch_activesync_sync_state](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_sync_state.md)
- [fetch_activesync_email_states](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_email_states.md)
- [fetch_activesync_email_states_by_ids](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_email_states_by_ids.md)
- [fetch_activesync_contact_states](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_contact_states.md)
- [fetch_activesync_contact_states_by_ids](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_contact_states_by_ids.md)
- [fetch_activesync_event_states](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_event_states.md)
- [fetch_activesync_event_states_by_ids](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_event_states_by_ids.md)
- [fetch_activesync_message_attachments](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_message_attachments.md)
- [fetch_activesync_attachment_content](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_attachment_content.md)
- [fetch_message_attachment_content_by_cid](../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_message_attachment_content_by_cid.md)
- [activesync_collection_kind](../../../../functions/crates/lpe-storage/src/activesync/activesync_collection_kind.md)
- [active_sync_device_state_from_row](../../../../functions/crates/lpe-storage/src/activesync/active_sync_device_state_from_row.md)
- [normalized_device_type](../../../../functions/crates/lpe-storage/src/activesync/normalized_device_type.md)
- [activesync_collection_kind_maps_builtin_and_mail_collections](../../../../functions/crates/lpe-storage/src/activesync/activesync_collection_kind_maps_builtin_and_mail_collections.md)

# Imports

- `anyhow::Result`
- `serde::Serialize`
- `serde_json::Value`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    ActiveSyncDeviceRow, ActiveSyncSyncStateRow, Storage,
}`
- `super::activesync_collection_kind`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)