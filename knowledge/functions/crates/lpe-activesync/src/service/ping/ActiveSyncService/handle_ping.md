---
type: Rust Method
title: handle_ping
resource: crates/lpe-activesync/src/service/ping.rs#L44-L143
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/load_ping_settings
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_settings_from_request
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_status_response
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/resolve_ping_collections
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/store_ping_settings
  - functions/crates/lpe-activesync/src/service/ping/ping_change_categories
  - functions/crates/lpe-activesync/src/service/ping/ping_deadline
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/wait_for_ping_change
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_ping( &self, principal: &AuthenticatedPrincipal, device_id: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [load_ping_settings](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/load_ping_settings.md)
- [ping_settings_from_request](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_settings_from_request.md)
- [ping_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_status_response.md)
- [folder_collections](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)
- [resolve_ping_collections](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/resolve_ping_collections.md)
- [store_ping_settings](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/store_ping_settings.md)
- [ping_change_categories](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ping_change_categories.md)
- [ping_deadline](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ping_deadline.md)
- [ping_requires_folder_sync](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync.md)
- [changed_ping_collections](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections.md)
- [wait_for_ping_change](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/wait_for_ping_change.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)