---
type: Rust Method
title: fetch_activesync_contact_states_by_ids
resource: crates/lpe-activesync/src/tests.rs#L1162-L1187
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_contact_states_by_ids<'a>( &'a self, _account_id: Uuid, ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>`