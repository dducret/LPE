---
type: Rust Method
title: fetch_activesync_sync_state
resource: crates/lpe-activesync/src/tests.rs#L1275-L1289
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_activesync_sync_state<'a>( &'a self, account_id: Uuid, device_id: &'a str, collection_id: &'a str, sync_key: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)