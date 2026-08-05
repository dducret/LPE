---
type: Rust Method
title: fetch_activesync_device
resource: crates/lpe-storage/src/activesync.rs#L104-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_activesync_device( &self, account_id: Uuid, device_id: &str, ) -> Result<Option<ActiveSyncDeviceState>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)