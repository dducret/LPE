---
type: Rust Module
title: service
resource: crates/lpe-jmap/src/service.rs#L1-L1411
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-body-body-bytes-extract-ws-websocketupgrade-defaultbodylimit-query-state-http-headermap-request-statuscode-middleware-self-next-response-intoresponse-response-routing-get-post-json-router
  - external/lpe-magika-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/lpe-storage-accessiblecontact-accessibleevent-auditentryinput-authenticatedaccount-clienttask-clienttasklist-collaborationcollection-jmapemail-jmapemailsubmission-jmapmailbox-jmapuploadblob-mailboxaccountaccess-mailboxrule-outlookprofilestate-searchfolderdefinition-senderidentity-storage-upsertsearchfolderinput
  - external/serde-json-json-map-value
  - external/sha2-digest-sha256
  - external/std-collections-hashmap-hashset-convert-infallible-sync-arc-oncelock
  - external/tokio-sync-ownedsemaphorepermit-semaphore
  - external/uuid-uuid
  - external/crate-convert-format-addresses-error-http-error-jmap-problem-method-error-method-error-from-error-set-error-jmap-problem-limit-jmap-problem-unknown-capability-eventsource-eventsourcequery-parse-parse-uuid-protocol-jmapapirequest-jmapapiresponse-jmapmethodcall-jmapmethodresponse-sessiondocument-session-state-changes-response-from-durable-with-cursor-changes-response-with-cursor-decode-query-state-encode-query-state-encode-query-state-reference-encode-state-encode-state-with-cursor-query-changes-response-from-diff-query-diff-for-kind-query-position-state-cursor-validate-query-state-token-durableobjectchange-stateentry-store-jmapshareinput-jmapstore-upload-message-rfc822-bytes-jmapblobid
  - external/helpers
  - external/pub-crate-use-helpers-collection-state-fingerprint-opaque-state-fingerprint-trim-snippet
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [router](../../../../functions/crates/lpe-jmap/src/service/router.md)
- [JmapService](../../../../classes/crates/lpe-jmap/src/service/JmapService.md)
- [new](../../../../functions/crates/lpe-jmap/src/service/JmapService/new.md)
- [new_with_validator](../../../../functions/crates/lpe-jmap/src/service/JmapService/new_with_validator.md)
- [session_handler](../../../../functions/crates/lpe-jmap/src/service/session_handler.md)
- [api_handler](../../../../functions/crates/lpe-jmap/src/service/api_handler.md)
- [api_concurrency_limit](../../../../functions/crates/lpe-jmap/src/service/api_concurrency_limit.md)
- [try_acquire_api_request_permit](../../../../functions/crates/lpe-jmap/src/service/try_acquire_api_request_permit.md)
- [upload_concurrency_limit](../../../../functions/crates/lpe-jmap/src/service/upload_concurrency_limit.md)
- [try_acquire_upload_request_permit](../../../../functions/crates/lpe-jmap/src/service/try_acquire_upload_request_permit.md)
- [upload_handler](../../../../functions/crates/lpe-jmap/src/service/upload_handler.md)
- [download_handler](../../../../functions/crates/lpe-jmap/src/service/download_handler.md)
- [websocket_handler](../../../../functions/crates/lpe-jmap/src/service/websocket_handler.md)
- [event_source_handler](../../../../functions/crates/lpe-jmap/src/service/event_source_handler.md)
- [requested_account_access](../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [handle_api_request](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)
- [handle_api_request_for_account](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_reminder_set](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)
- [handle_reminder_import_or_copy](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_import_or_copy.md)
- [apply_reminder_mutation](../../../../functions/crates/lpe-jmap/src/service/JmapService/apply_reminder_mutation.md)
- [handle_share_set](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)
- [handle_share_import_or_copy](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_import_or_copy.md)
- [handle_search_folder_set](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [handle_search_folder_import_or_copy](../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_import_or_copy.md)
- [authenticate](../../../../functions/crates/lpe-jmap/src/service/JmapService/authenticate.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{
    body::{Body, Bytes},
    extract::{ws::WebSocketUpgrade, DefaultBodyLimit, Query, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
}`
- `lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator}`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, AuditEntryInput, AuthenticatedAccount, ClientTask,
    ClientTaskList, CollaborationCollection, JmapEmail, JmapEmailSubmission, JmapMailbox,
    JmapUploadBlob, MailboxAccountAccess, MailboxRule, OutlookProfileState, SearchFolderDefinition,
    SenderIdentity, Storage, UpsertSearchFolderInput,
}`
- `serde_json::{json, Map, Value}`
- `sha2::{Digest, Sha256}`
- `std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::{Arc, OnceLock},
}`
- `tokio::sync::{OwnedSemaphorePermit, Semaphore}`
- `uuid::Uuid`
- `crate::{
    convert::format_addresses,
    error::{
        http_error, jmap_problem, method_error, method_error_from_error, set_error,
        JMAP_PROBLEM_LIMIT, JMAP_PROBLEM_UNKNOWN_CAPABILITY,
    },
    eventsource::EventSourceQuery,
    parse::parse_uuid,
    protocol::{
        JmapApiRequest, JmapApiResponse, JmapMethodCall, JmapMethodResponse, SessionDocument,
    },
    session,
    state::{
        changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state, encode_query_state_reference, encode_state,
        encode_state_with_cursor, query_changes_response_from_diff, query_diff_for_kind,
        query_position, state_cursor, validate_query_state_token, DurableObjectChange, StateEntry,
    },
    store::{JmapShareInput, JmapStore},
    upload::{message_rfc822_bytes, JmapBlobId},
}`
- `helpers::*`
- `pub(crate) use helpers::{collection_state_fingerprint, opaque_state_fingerprint, trim_snippet}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)