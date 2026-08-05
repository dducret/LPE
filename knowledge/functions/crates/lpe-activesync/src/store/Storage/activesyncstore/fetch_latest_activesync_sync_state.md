---
type: Rust Method
title: fetch_latest_activesync_sync_state
resource: crates/lpe-activesync/src/store.rs#L295-L305
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_latest_activesync_sync_state<'a>( &'a self, account_id: Uuid, device_id: &'a str, collection_id: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>`