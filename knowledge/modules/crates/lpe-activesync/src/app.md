---
type: Rust Module
title: app
resource: crates/lpe-activesync/src/app.rs#L1-L68
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-body-bytes-extract-rawquery-state-http-headermap-response-response-routing-on-methodfilter-router
  - external/lpe-mail-auth-authenticate-account
  - external/lpe-storage-storage
  - external/crate-constants-active-sync-path-response-auth-challenge-response-empty-response-error-response-service-activesyncservice-store-activesyncstore-types-activesyncquery-parsedactivesyncquery
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [router](../../../../functions/crates/lpe-activesync/src/app/router.md)
- [options_handler](../../../../functions/crates/lpe-activesync/src/app/options_handler.md)
- [options_response_for_store](../../../../functions/crates/lpe-activesync/src/app/options_response_for_store.md)
- [post_handler](../../../../functions/crates/lpe-activesync/src/app/post_handler.md)

# Imports

- `axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::Response,
    routing::{on, MethodFilter},
    Router,
}`
- `lpe_mail_auth::authenticate_account`
- `lpe_storage::Storage`
- `crate::{
    constants::ACTIVE_SYNC_PATH,
    response::{auth_challenge_response, empty_response, error_response},
    service::ActiveSyncService,
    store::ActiveSyncStore,
    types::{ActiveSyncQuery, ParsedActiveSyncQuery},
}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)