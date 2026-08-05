---
type: Rust Function
title: wbxml_response
resource: crates/lpe-activesync/src/response.rs#L34-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/response/add_common_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/service/search_status_response
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/folder_mutation_response
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_status_response
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision
  - functions/crates/lpe-activesync/src/service/provisioning/policy_required_response
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`pub(crate) fn wbxml_response(protocol_version: &str, body: Vec<u8>) -> Result<Response>`

# Calls

- [add_common_headers](../../../../../functions/crates/lpe-activesync/src/response/add_common_headers.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [handle_sync](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync.md)
- [command_status_response](../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [search_status_response](../../../../../functions/crates/lpe-activesync/src/service/search_status_response.md)
- [handle_folder_sync](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [folder_mutation_response](../../../../../functions/crates/lpe-activesync/src/service/folders/folder_mutation_response.md)
- [handle_get_item_estimate](../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate.md)
- [handle_item_operations](../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations.md)
- [handle_move_items](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items.md)
- [ping_status_response](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_status_response.md)
- [handle_provision](../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)
- [policy_required_response](../../../../../functions/crates/lpe-activesync/src/service/provisioning/policy_required_response.md)
- [handle_search](../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)
- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)