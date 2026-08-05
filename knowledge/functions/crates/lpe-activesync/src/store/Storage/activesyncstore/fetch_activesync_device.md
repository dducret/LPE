---
type: Rust Method
title: fetch_activesync_device
resource: crates/lpe-activesync/src/store.rs#L307-L313
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_device<'a>( &'a self, account_id: Uuid, device_id: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncDeviceState>>`