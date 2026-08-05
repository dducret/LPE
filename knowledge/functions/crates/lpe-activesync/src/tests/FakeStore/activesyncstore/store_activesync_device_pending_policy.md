---
type: Rust Method
title: store_activesync_device_pending_policy
resource: crates/lpe-activesync/src/tests.rs#L544-L567
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/device_key
---

# Signature

`fn store_activesync_device_pending_policy<'a>( &'a self, account_id: Uuid, device_id: &'a str, device_type: &'a str, pending_policy_key: &'a str, ) -> StoreFuture<'a, ()>`

# Calls

- [device_key](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)