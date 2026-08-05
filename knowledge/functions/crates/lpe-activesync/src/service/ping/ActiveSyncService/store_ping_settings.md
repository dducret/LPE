---
type: Rust Method
title: store_ping_settings
resource: crates/lpe-activesync/src/service/ping.rs#L290-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn store_ping_settings( &self, account_id: Uuid, device_id: &str, settings: &PingSettings, ) -> Result<()>`

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)