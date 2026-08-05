---
type: Rust Module
title: transport
resource: crates/lpe-exchange/src/mapi/transport.rs#L1-L1182
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-dispatch
  - external/super-identity-archive-folder-id-calendar-folder-id-common-views-folder-id-conflicts-folder-id-contacts-folder-id-contacts-search-folder-id-conversation-action-settings-folder-id-conversation-history-folder-id-deferred-action-folder-id-drafts-folder-id-freebusy-data-folder-id-inbox-folder-id-ipm-subtree-folder-id-journal-folder-id-junk-folder-id-local-failures-folder-id-notes-folder-id-outbox-folder-id-reminders-folder-id-root-folder-id-rss-feeds-folder-id-schedule-folder-id-search-folder-id-sent-folder-id-server-failures-folder-id-shortcuts-folder-id-spooler-queue-folder-id-suggested-contacts-folder-id-sync-issues-folder-id-tasks-folder-id-todo-search-folder-id-tracked-mail-processing-folder-id-trash-folder-id-views-folder-id
  - external/super-notifications
  - external/super-nspi
  - external/super-outlook-startup
  - external/super-rop
  - external/super-session
  - external/super-wire-mapihttprequesttype-as-mapirequesttype
  - external/super
  - external/lpe-core-outlook-trace-write-outlook-trace-outlooktracedirection-outlooktraceevent
  - external/lpe-domain-month-abbrev-utc-from-unix-seconds-weekday-abbrev-from-unix-days
  - external/pub-crate-use-cookies-request-cookie-transport-debug
  - external/pub-in-crate-mapi-use-cookies
  - external/diagnostics-log-connect-body-debug-log-mapi-session-disconnect
  - external/diagnostics-outlook-bootstrap-next-expected-phase-outlook-bootstrap-phase-outlook-bootstrap-phase-name-outlook-bootstrap-stall-code-outlook-bootstrap-stall-name-partial-scope-checkpoint-not-stored-count-post-fai-inbox-probe-loop-terminal-summary-required-default-folder-disconnect-coverage-summary-special-folder-contract-summary-summarize-connect-body
  - external/pub-in-crate-mapi-use-diagnostics-post-hierarchy-action-summary-visible-inbox-release-without-query-rows-observed
  - external/pub-in-crate-mapi-use-headers
  - external/pub-crate-use-headers-client-flow-key-debug-payload-preview-hex-guid-counter-debug-hex-preview-safe-header
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiEndpoint](../../../../../classes/crates/lpe-exchange/src/mapi/transport/MapiEndpoint.md)
- [handle_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [mapi_error_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response.md)
- [connect_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [log_mapi_session_establish](../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish.md)
- [connect_auxiliary_buffer](../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_auxiliary_buffer.md)
- [disconnect_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [disconnect_success_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response.md)
- [ping_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [mapi_diagnostic_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [mapi_diagnostic_response_with_cookies](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [mapi_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)
- [mapi_response_with_cookies](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [mapi_http_date](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date.md)
- [MapiResponseDebug](../../../../../classes/crates/lpe-exchange/src/mapi/transport/MapiResponseDebug.md)
- [mapi_response_payload_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload_bytes.md)
- [mapi_response_payload](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload.md)
- [finalize_mapi_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response.md)
- [log_mapi_connection](../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [trace_mapi_connection](../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [execute_response_trace_metadata](../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)
- [execute_response_rop_buffer_for_trace](../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_rop_buffer_for_trace.md)
- [execute_request_trace_metadata](../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata.md)
- [remote_peer](../../../../../functions/crates/lpe-exchange/src/mapi/transport/remote_peer.md)
- [execute_success_body](../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_success_body.md)
- [execute_transport_failure_response](../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_transport_failure_response.md)
- [insert_header](../../../../../functions/crates/lpe-exchange/src/mapi/transport/insert_header.md)
- [is_authentication_error](../../../../../functions/crates/lpe-exchange/src/mapi/transport/is_authentication_error.md)

# Imports

- `super::dispatch::*`
- `super::identity::{
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DRAFTS_FOLDER_ID,
    FREEBUSY_DATA_FOLDER_ID, INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID,
    JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID, NOTES_FOLDER_ID, OUTBOX_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID,
    SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
}`
- `super::notifications::*`
- `super::nspi::*`
- `super::outlook_startup::*`
- `super::rop::*`
- `super::session::*`
- `super::wire::MapiHttpRequestType as MapiRequestType`
- `super::*`
- `lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent}`
- `lpe_domain::{month_abbrev, utc_from_unix_seconds, weekday_abbrev_from_unix_days}`
- `pub(crate) use cookies::request_cookie_transport_debug`
- `pub(in crate::mapi) use cookies::*`
- `diagnostics::{log_connect_body_debug, log_mapi_session_disconnect}`
- `diagnostics::{
    outlook_bootstrap_next_expected_phase, outlook_bootstrap_phase, outlook_bootstrap_phase_name,
    outlook_bootstrap_stall_code, outlook_bootstrap_stall_name,
    partial_scope_checkpoint_not_stored_count, post_fai_inbox_probe_loop_terminal_summary,
    required_default_folder_disconnect_coverage_summary, special_folder_contract_summary,
    summarize_connect_body,
}`
- `pub(in crate::mapi) use diagnostics::{
    post_hierarchy_action_summary, visible_inbox_release_without_query_rows_observed,
}`
- `pub(in crate::mapi) use headers::*`
- `pub(crate) use headers::{
    client_flow_key, debug_payload_preview_hex, guid_counter_debug, hex_preview, safe_header,
}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)