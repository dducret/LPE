---
type: Rust Method
title: touch_activesync_device
resource: crates/lpe-activesync/src/tests.rs#L594-L608
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/device_key
---

# Signature

`fn touch_activesync_device<'a>( &'a self, account_id: Uuid, device_id: &'a str, ) -> StoreFuture<'a, ()>`

# Calls

- [device_key](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)