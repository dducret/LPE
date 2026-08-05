---
type: Rust Module
title: src
resource: crates/lpe-jmap/src/lib.rs#L1-L45
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-backbone-jmapaddressobject-jmapemailobject-jmapmailboxobject-jmapmailboxrights-jmapthreadobject
  - external/pub-use-crate-service-router-jmapservice
  - external/pub-crate-use-crate-convert-resolve-creation-reference
  - external/pub-crate-use-crate-parse-parse-submission-email-id
  - external/pub-crate-use-crate-service-collection-state-fingerprint-trim-snippet-default-get-limit-jmap-blob-capability-jmap-calendars-capability-jmap-contacts-capability-jmap-core-capability-jmap-lpe-outlook-capability-jmap-mail-capability-jmap-submission-capability-jmap-tasks-capability-jmap-vacation-response-capability-jmap-websocket-capability-max-blob-data-sources-max-calls-in-request-max-concurrent-requests-max-concurrent-upload-max-objects-in-get-max-objects-in-set-max-query-limit-max-size-request-max-size-upload-push-state-version-query-state-version-session-state-state-token-version
  - external/pub-crate-use-crate-session-requested-account-id
  - external/pub-crate-use-crate-state-encode-query-state
  - external/pub-crate-use-crate-upload-blob-id-for-message
  member_of:
  - packages/crates/lpe-jmap
---

# Imports

- `pub use crate::backbone::{
    JmapAddressObject, JmapEmailObject, JmapMailboxObject, JmapMailboxRights, JmapThreadObject,
}`
- `pub use crate::service::{router, JmapService}`
- `pub(crate) use crate::convert::resolve_creation_reference`
- `pub(crate) use crate::parse::parse_submission_email_id`
- `pub(crate) use crate::service::{
    collection_state_fingerprint, trim_snippet, DEFAULT_GET_LIMIT, JMAP_BLOB_CAPABILITY,
    JMAP_CALENDARS_CAPABILITY, JMAP_CONTACTS_CAPABILITY, JMAP_CORE_CAPABILITY,
    JMAP_LPE_OUTLOOK_CAPABILITY, JMAP_MAIL_CAPABILITY, JMAP_SUBMISSION_CAPABILITY,
    JMAP_TASKS_CAPABILITY, JMAP_VACATION_RESPONSE_CAPABILITY, JMAP_WEBSOCKET_CAPABILITY,
    MAX_BLOB_DATA_SOURCES, MAX_CALLS_IN_REQUEST, MAX_CONCURRENT_REQUESTS, MAX_CONCURRENT_UPLOAD,
    MAX_OBJECTS_IN_GET, MAX_OBJECTS_IN_SET, MAX_QUERY_LIMIT, MAX_SIZE_REQUEST, MAX_SIZE_UPLOAD,
    PUSH_STATE_VERSION, QUERY_STATE_VERSION, SESSION_STATE, STATE_TOKEN_VERSION,
}`
- `pub(crate) use crate::session::requested_account_id`
- `pub(crate) use crate::state::encode_query_state`
- `pub(crate) use crate::upload::blob_id_for_message`

# Member of

- [lpe-jmap](../../../packages/crates/lpe-jmap.md)