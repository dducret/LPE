---
type: Rust Module
title: search
resource: crates/lpe-activesync/src/service/search.rs#L1-L152
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-response-response
  - external/crate-message-field-text-protocol-activesyncstatus-response-wbxml-response-snapshot-email-application-data-bodypreference-store-activesyncstore-types-authenticatedprincipal-wbxml-encode-wbxml-wbxmlnode
  - external/super-search-status-response-value-to-wbxml-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_search](../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)
- [search_query_text](../../../../../functions/crates/lpe-activesync/src/service/search/search_query_text.md)
- [parse_range](../../../../../functions/crates/lpe-activesync/src/service/search/parse_range.md)
- [trim_preview](../../../../../functions/crates/lpe-activesync/src/service/search/trim_preview.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::response::Response`
- `crate::{
    message::field_text,
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::{email_application_data, BodyPreference},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
}`
- `super::{search_status_response, value_to_wbxml, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)