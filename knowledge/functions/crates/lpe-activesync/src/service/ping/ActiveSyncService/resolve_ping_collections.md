---
type: Rust Method
title: resolve_ping_collections
resource: crates/lpe-activesync/src/service/ping.rs#L307-L333
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn resolve_ping_collections( &self, principal: &AuthenticatedPrincipal, device_id: &str, collections: &[CollectionDefinition], folders: &[PingFolder], ) -> Result<PingResolution>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [decode_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)