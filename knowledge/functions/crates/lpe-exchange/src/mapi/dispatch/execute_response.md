---
type: Rust Function
title: execute_response
resource: crates/lpe-exchange/src/mapi/dispatch.rs#L204-L703
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/execute_transport_failure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/acquire_execute_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_parse_failure_debug
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/mapi_payload_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_request_start_debug
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_success_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_successful_execute_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_dispatch_start_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/transport/execute_success_body
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/cache_execute_response
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_targets
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope/request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/replica_guid
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_collaboration_hierarchy_notification_requires_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_store_access_debug
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) async fn execute_response<S, V>( store: &S, validator: &Validator<V>, endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, body: &[u8], request_id: &str, ) -> Response where S: ExchangeStore, V: Detector,`

# Calls

- [log_session_cookie_lookup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [request_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [execute_transport_failure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_transport_failure_response.md)
- [acquire_execute_active_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/acquire_execute_active_session_request.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [get_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [record_transport_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request.md)
- [parse_execute_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request.md)
- [log_execute_parse_failure_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_parse_failure_debug.md)
- [session_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [mapi_payload_fingerprint](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/mapi_payload_fingerprint.md)
- [summarize_request_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [log_execute_request_start_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_request_start_debug.md)
- [hierarchy_sync_completed](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_execute_after_hierarchy_completion](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion.md)
- [execute_success_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_success_rop_buffer.md)
- [log_execute_rop_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [summarize_response_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)
- [record_last_successful_execute_context](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_successful_execute_context.md)
- [log_post_common_views_handoff_execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [store_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [execute_can_skip_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope.md)
- [empty](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes.md)
- [emails](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails.md)
- [log_execute_dispatch_start_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_dispatch_start_debug.md)
- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [apply_execute_max_rop_out](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out.md)
- [execute_success_body](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_success_body.md)
- [cache_execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/cache_execute_response.md)
- [has_notification_targets](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_targets.md)
- [fetch_mapi_notification_cursor](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [load_mapi_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [request_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope/request_identity_scope.md)
- [replica_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/replica_guid.md)
- [refresh_persisted_special_folder_aliases](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases.md)
- [with_current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [plan_mapi_store_access](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [pending_collaboration_hierarchy_notification_requires_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_collaboration_hierarchy_notification_requires_contents.md)
- [log_execute_store_access_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_store_access_debug.md)
- [with_current_mapi_request_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope.md)
- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)
- [identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)

# Called by

- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)