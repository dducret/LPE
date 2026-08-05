---
type: Rust Module
title: session
resource: crates/lpe-jmap/src/session.rs#L1-L324
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-http-headermap
  - external/serde-json-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/lpe-storage-authenticatedaccount-mailboxaccountaccess
  - external/crate-parse-parse-uuid-protocol-sessionaccount-sessiondocument-service-opaque-state-fingerprint-jmapservice-jmap-blob-capability-jmap-calendars-capability-jmap-contacts-capability-jmap-core-capability-jmap-lpe-outlook-capability-jmap-mail-capability-jmap-submission-capability-jmap-tasks-capability-jmap-vacation-response-capability-jmap-websocket-capability-max-blob-data-sources-max-calls-in-request-max-concurrent-requests-max-concurrent-upload-max-objects-in-get-max-objects-in-set-max-size-request-max-size-upload-session-state
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [session_document](../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)
- [public_base_url](../../../../functions/crates/lpe-jmap/src/session/public_base_url.md)
- [public_base_path](../../../../functions/crates/lpe-jmap/src/session/public_base_path.md)
- [websocket_url](../../../../functions/crates/lpe-jmap/src/session/websocket_url.md)
- [normalize_public_base_path](../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_path.md)
- [normalize_public_base_url](../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_url.md)
- [session_capabilities](../../../../functions/crates/lpe-jmap/src/session/session_capabilities.md)
- [requested_account_id](../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [mailbox_account_is_read_only](../../../../functions/crates/lpe-jmap/src/session/mailbox_account_is_read_only.md)
- [session_account_capabilities](../../../../functions/crates/lpe-jmap/src/session/session_account_capabilities.md)
- [account_capability_value](../../../../functions/crates/lpe-jmap/src/session/account_capability_value.md)
- [session_state](../../../../functions/crates/lpe-jmap/src/session/session_state.md)
- [session_account_version](../../../../functions/crates/lpe-jmap/src/session/session_account_version.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::http::HeaderMap`
- `serde_json::{json, Value}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `lpe_storage::{AuthenticatedAccount, MailboxAccountAccess}`
- `crate::{
    parse::parse_uuid,
    protocol::{SessionAccount, SessionDocument},
    service::opaque_state_fingerprint,
    JmapService, JMAP_BLOB_CAPABILITY, JMAP_CALENDARS_CAPABILITY, JMAP_CONTACTS_CAPABILITY,
    JMAP_CORE_CAPABILITY, JMAP_LPE_OUTLOOK_CAPABILITY, JMAP_MAIL_CAPABILITY,
    JMAP_SUBMISSION_CAPABILITY, JMAP_TASKS_CAPABILITY, JMAP_VACATION_RESPONSE_CAPABILITY,
    JMAP_WEBSOCKET_CAPABILITY, MAX_BLOB_DATA_SOURCES, MAX_CALLS_IN_REQUEST,
    MAX_CONCURRENT_REQUESTS, MAX_CONCURRENT_UPLOAD, MAX_OBJECTS_IN_GET, MAX_OBJECTS_IN_SET,
    MAX_SIZE_REQUEST, MAX_SIZE_UPLOAD, SESSION_STATE,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)