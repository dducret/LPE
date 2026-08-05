---
type: Rust Module
title: provisioning
resource: crates/lpe-activesync/src/service/provisioning.rs#L1-L189
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-http-headermap-response-response
  - external/uuid-uuid
  - external/crate-protocol-activesynccommand-activesyncstatus-response-policy-key-wbxml-response-store-activesyncstore-types-authenticatedprincipal-wbxml-encode-wbxml-wbxmlnode
  - external/super-command-status-response-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_provision](../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)
- [policy_key_is_current](../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/policy_key_is_current.md)
- [header_policy_key](../../../../../functions/crates/lpe-activesync/src/service/provisioning/header_policy_key.md)
- [policy_required_response](../../../../../functions/crates/lpe-activesync/src/service/provisioning/policy_required_response.md)

# Imports

- `anyhow::Result`
- `axum::{http::HeaderMap, response::Response}`
- `uuid::Uuid`
- `crate::{
    protocol::{ActiveSyncCommand, ActiveSyncStatus},
    response::{policy_key, wbxml_response},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{command_status_response, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)