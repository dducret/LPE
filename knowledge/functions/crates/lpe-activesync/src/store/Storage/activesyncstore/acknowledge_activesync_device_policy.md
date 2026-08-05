---
type: Rust Method
title: acknowledge_activesync_device_policy
resource: crates/lpe-activesync/src/store.rs#L333-L349
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn acknowledge_activesync_device_policy<'a>( &'a self, account_id: Uuid, device_id: &'a str, device_type: &'a str, policy_key: &'a str, ) -> StoreFuture<'a, ()>`