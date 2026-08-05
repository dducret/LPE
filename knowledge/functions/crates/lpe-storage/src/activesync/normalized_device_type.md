---
type: Rust Function
title: normalized_device_type
resource: crates/lpe-storage/src/activesync.rs#L722-L729
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/activesync/Storage/store_activesync_device_pending_policy
  - functions/crates/lpe-storage/src/activesync/Storage/acknowledge_activesync_device_policy
---

# Signature

`fn normalized_device_type(device_type: &str) -> String`

# Called by

- [store_activesync_device_pending_policy](../../../../../functions/crates/lpe-storage/src/activesync/Storage/store_activesync_device_pending_policy.md)
- [acknowledge_activesync_device_policy](../../../../../functions/crates/lpe-storage/src/activesync/Storage/acknowledge_activesync_device_policy.md)