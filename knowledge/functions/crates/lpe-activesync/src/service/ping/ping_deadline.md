---
type: Rust Function
title: ping_deadline
resource: crates/lpe-activesync/src/service/ping.rs#L359-L361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`fn ping_deadline(heartbeat_interval: u32) -> Instant`

# Called by

- [handle_ping](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)