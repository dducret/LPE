---
type: Rust Method
title: fetch_activesync_device
resource: crates/lpe-activesync/src/tests.rs#L530-L542
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/tests/FakeStore/device_key
---

# Signature

`fn fetch_activesync_device<'a>( &'a self, account_id: Uuid, device_id: &'a str, ) -> StoreFuture<'a, Option<ActiveSyncDeviceState>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [device_key](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)