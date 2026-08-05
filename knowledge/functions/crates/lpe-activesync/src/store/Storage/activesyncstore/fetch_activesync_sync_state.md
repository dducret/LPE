---
type: Rust Method
title: fetch_activesync_sync_state
resource: crates/lpe-activesync/src/store.rs#L636-L647
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_sync_state<'a>( &'a self, account_id: Uuid, device_id: &'a str, collection_id: &'a str, sync_key: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>`