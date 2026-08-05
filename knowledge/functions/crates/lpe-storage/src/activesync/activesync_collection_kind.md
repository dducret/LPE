---
type: Rust Function
title: activesync_collection_kind
resource: crates/lpe-storage/src/activesync.rs#L698-L706
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/activesync/Storage/store_activesync_sync_state
  - functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_sync_state
  - functions/crates/lpe-storage/src/message_ops/Storage/fetch_latest_activesync_sync_state
---

# Signature

`pub(crate) fn activesync_collection_kind(collection_id: &str) -> &'static str`

# Called by

- [store_activesync_sync_state](../../../../../functions/crates/lpe-storage/src/activesync/Storage/store_activesync_sync_state.md)
- [fetch_activesync_sync_state](../../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_sync_state.md)
- [fetch_latest_activesync_sync_state](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/fetch_latest_activesync_sync_state.md)