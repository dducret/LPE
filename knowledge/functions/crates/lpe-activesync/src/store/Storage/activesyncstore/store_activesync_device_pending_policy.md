---
type: Rust Method
title: store_activesync_device_pending_policy
resource: crates/lpe-activesync/src/store.rs#L315-L331
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn store_activesync_device_pending_policy<'a>( &'a self, account_id: Uuid, device_id: &'a str, device_type: &'a str, pending_policy_key: &'a str, ) -> StoreFuture<'a, ()>`