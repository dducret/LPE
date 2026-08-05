---
type: Rust Module
title: item_operations
resource: crates/lpe-activesync/src/service/item_operations.rs#L1-L193
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/axum-response-response
  - external/uuid-uuid
  - external/crate-protocol-activesyncstatus-bodypreferencetype-response-wbxml-response-snapshot-email-application-data-store-activesyncstore-types-authenticatedprincipal-wbxml-encode-wbxml-wbxmlnode
  - external/super-command-status-response-fetch-body-preference-value-to-wbxml-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_item_operations](../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations.md)
- [handle_item_operations_fetch](../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)

# Imports

- `anyhow::{anyhow, Result}`
- `axum::response::Response`
- `uuid::Uuid`
- `crate::{
    protocol::{ActiveSyncStatus, BodyPreferenceType},
    response::wbxml_response,
    snapshot::email_application_data,
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{command_status_response, fetch_body_preference, value_to_wbxml, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)