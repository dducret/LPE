---
type: Rust Method
title: store_activesync_device_pending_policy
resource: crates/lpe-storage/src/activesync.rs#L130-L165
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/activesync/normalized_device_type
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn store_activesync_device_pending_policy( &self, account_id: Uuid, device_id: &str, device_type: &str, pending_policy_key: &str, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [normalized_device_type](../../../../../../functions/crates/lpe-storage/src/activesync/normalized_device_type.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)