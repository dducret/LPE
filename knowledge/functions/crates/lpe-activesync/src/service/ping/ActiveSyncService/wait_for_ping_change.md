---
type: Rust Method
title: wait_for_ping_change
resource: crates/lpe-activesync/src/service/ping.rs#L184-L203
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn wait_for_ping_change( &self, change_listener: &mut Option<CanonicalChangeListener>, categories: &[CanonicalChangeCategory], remaining: Duration, ) -> Result<()>`

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)