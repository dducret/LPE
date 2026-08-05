---
type: Rust Function
title: decode_sync_state
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L10-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/service/sync_helpers/completed_sync_state
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/resolve_ping_collections
---

# Signature

`pub(super) fn decode_sync_state(snapshot_json: &str) -> Result<StoredSyncState>`

# Calls

- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [completed_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/completed_sync_state.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [get_item_estimate_response](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
- [resolve_ping_collections](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/resolve_ping_collections.md)