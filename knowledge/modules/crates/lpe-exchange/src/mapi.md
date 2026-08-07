---
type: Rust Module
title: mapi
resource: crates/lpe-exchange/src/mapi.rs#L1-L344
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/axum-http-header-content-type-set-cookie-www-authenticate-headermap-headervalue-statuscode-response-intoresponse-response
  - external/lpe-magika-detector-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/lpe-mail-auth-authenticate-account-accountprincipal
  - external/lpe-storage-accessiblecontact-accessibleevent-attachmentuploadinput-auditentryinput-calendarparticipantsmetadata-clientnote-clienttask-collaborationrights-jmapemail-jmapemailaddress-jmapimportedemailinput-jmapmailbox-journalentry-submitmessageinput-submittedmessage-submittedrecipientinput-upsertclientcontactinput-upsertclienteventinput-upsertclientnoteinput-upsertclienttaskinput-upsertjournalentryinput
  - external/std-cmp-ordering-collections-hashmap-hashset-vecdeque-env-sync-atomic-atomicu64-ordering-as-atomicordering-mutex-oncelock-time-duration-systemtime
  - external/tracing-warn
  - external/uuid-uuid
  - external/crate-mapi-mailstore-mapi-store-mapiattachment-mapicollaborationfolder-mapicollaborationfolderkind-mapimailstoresnapshot-mapistore-store-exchangeaddressbookdirectorykind-exchangeaddressbookentry-exchangeaddressbookentrykind-exchangestore-mapicheckpointkind-mapiidentityobjectkind-mapiidentityrequest
  - external/pub-crate-use-crate-mapi-session-create-rpc-emsmdb-context-execute-rpc-emsmdb-rops-transport-client-flow-key-debug-payload-preview-hex-guid-counter-debug-handle-mapi-mapi-error-response-mapi-response-payload-bytes-request-cookie-transport-debug-safe-header-mapiendpoint
  - external/pub-use-event-metrics-mapi-calendar-event-save-metrics-mapicalendareventsavemetrics
  - external/pub-crate-use-event-metrics-record-mapi-calendar-event-save-mapicalendareventsaveflow-mapicalendareventsaveoutcome
  - external/pub-use-notification-metrics-mapi-notification-metrics-mapinotificationmetrics
  - external/pub-crate-use-notification-metrics-record-mapi-new-mail-notification-deliveries-record-mapi-notification-wait-completion-mapinotificationwaitoutcome
  - external/pub-crate-use-crate-mapi-session-begin-active-session-request-for-test
  - external/pub-crate-use-crate-mapi-store-adapter-load-mapi-identity-codec-for-test
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiFolderPurgeMetrics](../../../../classes/crates/lpe-exchange/src/mapi/MapiFolderPurgeMetrics.md)
- [MapiOutlookViewMetrics](../../../../classes/crates/lpe-exchange/src/mapi/MapiOutlookViewMetrics.md)
- [record_mapi_folder_purge_metrics](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_folder_purge_metrics.md)
- [mapi_folder_purge_metrics](../../../../functions/crates/lpe-exchange/src/mapi/mapi_folder_purge_metrics.md)
- [record_mapi_outlook_view_inbox_fai_handoff_without_contents](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_fai_handoff_without_contents.md)
- [record_mapi_outlook_view_common_views_handoff_without_contents](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_common_views_handoff_without_contents.md)
- [record_mapi_outlook_view_post_common_views_inbox_notification_without_contents](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_common_views_inbox_notification_without_contents.md)
- [record_mapi_outlook_view_repeated_inbox_open_after_common_views](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_repeated_inbox_open_after_common_views.md)
- [record_mapi_outlook_view_post_fai_hierarchy_without_contents](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_fai_hierarchy_without_contents.md)
- [record_mapi_outlook_view_inbox_normal_contents_opened](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_normal_contents_opened.md)
- [record_mapi_outlook_view_ipm_subtree_hierarchy_query](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_ipm_subtree_hierarchy_query.md)
- [record_mapi_outlook_view_bootstrap_progress](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_progress.md)
- [record_mapi_outlook_view_bootstrap_stall](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall.md)
- [record_mapi_outlook_view_post_calendar_query_position_named_property_probe](../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_post_calendar_query_position_named_property_probe.md)
- [mapi_outlook_view_metrics](../../../../functions/crates/lpe-exchange/src/mapi/mapi_outlook_view_metrics.md)

# Imports

- `anyhow::{anyhow, Result}`
- `axum::{
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
}`
- `lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
}`
- `lpe_mail_auth::{authenticate_account, AccountPrincipal}`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, AttachmentUploadInput, AuditEntryInput,
    CalendarParticipantsMetadata, ClientNote, ClientTask, CollaborationRights, JmapEmail,
    JmapEmailAddress, JmapImportedEmailInput, JmapMailbox, JournalEntry, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput,
}`
- `std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    env,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Mutex, OnceLock,
    },
    time::{Duration, SystemTime},
}`
- `tracing::warn`
- `uuid::Uuid`
- `crate::{
    mapi_mailstore,
    mapi_store::{
        MapiAttachment, MapiCollaborationFolder, MapiCollaborationFolderKind,
        MapiMailStoreSnapshot, MapiStore,
    },
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry, ExchangeAddressBookEntryKind,
        ExchangeStore, MapiCheckpointKind, MapiIdentityObjectKind, MapiIdentityRequest,
    },
}`
- `pub(crate) use crate::mapi::{
    session::{create_rpc_emsmdb_context, execute_rpc_emsmdb_rops},
    transport::{
        client_flow_key, debug_payload_preview_hex, guid_counter_debug, handle_mapi,
        mapi_error_response, mapi_response_payload_bytes, request_cookie_transport_debug,
        safe_header, MapiEndpoint,
    },
}`
- `pub use event_metrics::{mapi_calendar_event_save_metrics, MapiCalendarEventSaveMetrics}`
- `pub(crate) use event_metrics::{
    record_mapi_calendar_event_save, MapiCalendarEventSaveFlow, MapiCalendarEventSaveOutcome,
}`
- `pub use notification_metrics::{mapi_notification_metrics, MapiNotificationMetrics}`
- `pub(crate) use notification_metrics::{
    record_mapi_new_mail_notification_deliveries, record_mapi_notification_wait_completion,
    MapiNotificationWaitOutcome,
}`
- `pub(crate) use crate::mapi::session::begin_active_session_request_for_test`
- `pub(crate) use crate::mapi::store_adapter::load_mapi_identity_codec_for_test`

# Member of

- [lpe-exchange](../../../../packages/crates/lpe-exchange.md)