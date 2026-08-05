---
type: Rust Method
title: fetch_activesync_event_states_by_ids
resource: crates/lpe-activesync/src/store.rs#L605-L614
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_event_states_by_ids<'a>( &'a self, account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>`