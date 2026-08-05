---
type: Rust Module
title: response
resource: crates/lpe-activesync/src/response.rs#L1-L112
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-http-header-content-type-www-authenticate-headermap-headervalue-statuscode-response-response
  - external/uuid-uuid
  - external/crate-constants-active-sync-commands-active-sync-version-wbxml-wbxmlnode
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [empty_response](../../../../functions/crates/lpe-activesync/src/response/empty_response.md)
- [auth_challenge_response](../../../../functions/crates/lpe-activesync/src/response/auth_challenge_response.md)
- [wbxml_response](../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)
- [add_common_headers](../../../../functions/crates/lpe-activesync/src/response/add_common_headers.md)
- [error_response](../../../../functions/crates/lpe-activesync/src/response/error_response.md)
- [is_authentication_error](../../../../functions/crates/lpe-activesync/src/response/is_authentication_error.md)
- [sync_status_node](../../../../functions/crates/lpe-activesync/src/response/sync_status_node.md)
- [policy_key](../../../../functions/crates/lpe-activesync/src/response/policy_key.md)
- [is_message_rfc822](../../../../functions/crates/lpe-activesync/src/response/is_message_rfc822.md)

# Imports

- `anyhow::Result`
- `axum::{
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::Response,
}`
- `uuid::Uuid`
- `crate::{
    constants::{ACTIVE_SYNC_COMMANDS, ACTIVE_SYNC_VERSION},
    wbxml::WbxmlNode,
}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)