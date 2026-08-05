---
type: Rust Method
title: store_activesync_sync_state
resource: crates/lpe-activesync/src/store.rs#L616-L634
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn store_activesync_sync_state<'a>( &'a self, account_id: Uuid, device_id: &'a str, collection_id: &'a str, sync_key: &'a str, snapshot_json: String, ) -> StoreFuture<'a, ()>`