---
type: Rust Module
title: move_items
resource: crates/lpe-activesync/src/service/move_items.rs#L1-L137
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-response-response
  - external/lpe-storage-auditentryinput
  - external/uuid-uuid
  - external/crate-protocol-activesyncstatus-response-wbxml-response-snapshot-mail-collection-parse-collection-mailbox-id-store-activesyncstore-types-authenticatedprincipal-wbxml-encode-wbxml-wbxmlnode
  - external/super-command-status-response-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_move_items](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items.md)
- [handle_move_item](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)

# Imports

- `anyhow::Result`
- `axum::response::Response`
- `lpe_storage::AuditEntryInput`
- `uuid::Uuid`
- `crate::{
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::{mail_collection, parse_collection_mailbox_id},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{command_status_response, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)