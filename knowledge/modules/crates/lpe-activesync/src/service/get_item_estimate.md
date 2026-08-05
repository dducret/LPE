---
type: Rust Module
title: get_item_estimate
resource: crates/lpe-activesync/src/service/get_item_estimate.rs#L1-L107
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-response-response
  - external/crate-protocol-activesyncstatus-response-wbxml-response-snapshot-diff-collection-states-store-activesyncstore-types-authenticatedprincipal-wbxml-encode-wbxml-wbxmlnode
  - external/super-decode-sync-state-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_get_item_estimate](../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate.md)
- [get_item_estimate_response](../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::response::Response`
- `crate::{
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::diff_collection_states,
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{decode_sync_state, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)