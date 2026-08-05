---
type: Rust Method
title: load_ping_settings
resource: crates/lpe-activesync/src/service/ping.rs#L278-L288
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn load_ping_settings( &self, account_id: Uuid, device_id: &str, ) -> Result<Option<PingSettings>>`

# Calls

- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)