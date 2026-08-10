---
type: Rust Method
title: device_key
resource: crates/lpe-activesync/src/tests.rs#L247-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_device
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/store_activesync_device_pending_policy
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/acknowledge_activesync_device_policy
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/touch_activesync_device
  - functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document
  - functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key
---

# Signature

`fn device_key(account_id: Uuid, device_id: &str) -> String`

# Called by

- [fetch_activesync_device](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_device.md)
- [store_activesync_device_pending_policy](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/store_activesync_device_pending_policy.md)
- [acknowledge_activesync_device_policy](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/acknowledge_activesync_device_policy.md)
- [touch_activesync_device](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/touch_activesync_device.md)
- [provision_returns_policy_key_and_lightweight_policy_document](../../../../../../functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)