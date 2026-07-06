use super::notifications::*;
use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::store_adapter::*;
use super::sync::*;
use super::tables::*;
use super::transport::*;
use super::wire::{MapiPropertyType, MapiSyncType, RopId};
use super::*;
use crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
};
use crate::store::{
    MapiCustomPropertyObjectKind, MapiCustomPropertyValue, MapiIdentityObjectKind,
    MapiSyncChangeSet, MapiSyncCheckpoint, UpsertMapiAssociatedConfigInput,
    UpsertMapiNavigationShortcutInput,
};
use lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent};
use lpe_domain::current_windows_filetime;
use lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, JmapEmail, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, PublicFolderPermissionInput, SearchFolderDefinition,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;

mod associated_config;
mod attachments;
mod contacts;
mod conversation_actions;
mod custom_properties;
mod default_folders;
mod diagnostics;
mod execute;
mod folder_create;
mod folder_dispatch;
mod folder_open;
mod folders;
mod logon;
mod message_dispatch;
mod message_open;
mod message_save;
mod message_state;
mod messages;
mod named_properties;
mod notification_subscriptions;
mod object_ids;
mod permissions;
mod properties;
mod property_dispatch;
mod property_mutations;
mod property_tags;
mod public_folders;
mod recipients;
mod recoverable_items;
mod release;
mod rules;
mod search_folders;
mod stream_dispatch;
mod submission;
mod sync_configure;
mod sync_get_buffer;
mod sync_import;
mod sync_import_deletes;
mod sync_import_hierarchy;
mod sync_import_message;
mod sync_import_message_move;
mod sync_import_read_state;
mod sync_transfer;
mod sync_upload_state;
mod table_controls;
mod table_diagnostics;
mod table_open;
mod table_validation;
mod tables;
mod unsupported;

use associated_config::*;
use attachments::*;
use contacts::*;
use conversation_actions::*;
use custom_properties::*;
use default_folders::*;
pub(in crate::mapi) use diagnostics::*;
pub(in crate::mapi) use execute::*;
use folder_create::*;
use folder_dispatch::*;
use folder_open::*;
use folders::*;
use logon::*;
use message_dispatch::*;
use message_open::*;
use message_save::*;
use message_state::*;
use messages::*;
use named_properties::*;
use notification_subscriptions::*;
use object_ids::*;
use permissions::*;
use properties::*;
use property_dispatch::*;
use property_mutations::*;
use property_tags::*;
use public_folders::*;
use recipients::*;
use recoverable_items::*;
use release::*;
use rules::*;
use search_folders::*;
use stream_dispatch::*;
use submission::*;
use sync_configure::*;
use sync_get_buffer::*;
use sync_import::*;
use sync_import_deletes::*;
use sync_import_hierarchy::*;
use sync_import_message::*;
use sync_import_message_move::*;
use sync_import_read_state::*;
use sync_transfer::*;
use sync_upload_state::*;
use table_controls::*;
use table_diagnostics::*;
use table_open::*;
use table_validation::*;
use tables::*;
use unsupported::*;

const EC_SEARCH_UNSUPPORTED: u32 = 0x8004_0102;
const EC_SEARCH_NOT_FOUND: u32 = 0x8004_010F;
const EC_SEARCH_SCOPE_VIOLATION: u32 = 0x0000_0490;
const EC_SEARCH_ACCESS_DENIED: u32 = 0x8007_0005;
const EC_SEARCH_NOT_INITIALIZED: u32 = 0x8004_0605;
const EC_SEARCH_INVALID_PARAMETER: u32 = 0x8007_0057;
const SEARCH_RUNNING_FLAG: u32 = 0x0000_0001;
const SEARCH_RECURSIVE_FLAG: u32 = 0x0000_0004;
const SET_SEARCH_STOP_FLAG: u32 = 0x0000_0001;
const SET_SEARCH_RESTART_FLAG: u32 = 0x0000_0002;
const SET_SEARCH_SHALLOW_FLAG: u32 = 0x0000_0008;
const SET_SEARCH_BACKGROUND_FLAG: u32 = 0x0000_0020;
const SET_SEARCH_CONTENT_INDEXED_FLAG: u32 = 0x0001_0000;
const SET_SEARCH_NON_CONTENT_INDEXED_FLAG: u32 = 0x0002_0000;
const SET_SEARCH_STATIC_FLAG: u32 = 0x0004_0000;
const SET_SEARCH_VALID_FLAGS: u32 = SET_SEARCH_STOP_FLAG
    | SET_SEARCH_RESTART_FLAG
    | SEARCH_RECURSIVE_FLAG
    | SET_SEARCH_SHALLOW_FLAG
    | SET_SEARCH_BACKGROUND_FLAG
    | SET_SEARCH_CONTENT_INDEXED_FLAG
    | SET_SEARCH_NON_CONTENT_INDEXED_FLAG
    | SET_SEARCH_STATIC_FLAG;
const EC_RULE_UNSUPPORTED: u32 = 0x8004_0102;
const EC_RULE_NOT_FOUND: u32 = 0x8004_010F;
const EC_RULE_INVALID_PARAMETER: u32 = 0x8007_0057;
const SYNC_SEND_OPTION_RECOVER_MODE: u8 = 0x04;
const SYNC_SEND_OPTION_PARTIAL_ITEM: u8 = 0x10;
const DEFAULT_CALENDAR_COLLECTION_ID: &str = "default";
const ROW_ADD: u8 = 0x01;
const ROW_MODIFY: u8 = 0x02;
const ROW_REMOVE: u8 = 0x04;
const PID_TAG_RULE_ID: u32 = 0x6674_0014;
const PID_TAG_RULE_STATE: u32 = 0x6677_0003;
const PID_TAG_RULE_CONDITION: u32 = 0x6679_00FD;
const PID_TAG_RULE_ACTIONS: u32 = 0x6680_00FE;
const PID_TAG_RULE_NAME_W: u32 = 0x6682_001F;
const PID_TAG_RULE_PROVIDER_DATA: u32 = 0x6684_0102;
const ST_ENABLED: u32 = 0x0000_0001;

pub(in crate::mapi) async fn execute_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
    V: Detector,
{
    log_session_cookie_lookup(endpoint, principal, headers, "Execute");
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return execute_failure_response(request_id, 13, "missing MAPI session cookie", None);
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return execute_failure_response(
            request_id,
            6,
            "invalid MAPI request sequence cookie",
            None,
        );
    }
    let Some(_active_request) = acquire_execute_active_session_request(&session_id).await else {
        return execute_failure_response(
            request_id,
            15,
            "MAPI session already has an active request",
            None,
        );
    };
    let Some(mut session) = get_session(&session_id) else {
        return execute_failure_response(request_id, 10, "MAPI session context not found", None);
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            None,
        );
    }
    session.record_transport_request("Execute", request_id);

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            log_execute_parse_failure_debug(endpoint, principal, headers, request_id, body, &error);
            return execute_failure_response(
                request_id,
                4,
                &format!("invalid Execute request body: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }
    let rop_fingerprint = mapi_payload_fingerprint(&execute.rop_buffer);
    let request_debug = summarize_request_rop_buffer(&execute.rop_buffer);
    log_execute_request_start_debug(
        endpoint,
        principal,
        headers,
        request_id,
        body.len(),
        &execute.rop_buffer,
        &request_debug,
    );
    let hierarchy_completed_before_execute = session.hierarchy_sync_completed();
    if let Some(cached) = session.completed_execute_requests.get(request_id).cloned() {
        if cached.rop_fingerprint == rop_fingerprint {
            let post_hierarchy_observation =
                if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                    session.record_execute_after_hierarchy_completion(
                        &request_debug.ids,
                        &request_debug.names_csv,
                    )
                } else {
                    PostHierarchyExecuteObservation::default()
                };
            let cached_rop_buffer = execute_success_rop_buffer(&cached.response_body);
            log_execute_rop_debug(
                endpoint,
                principal,
                headers,
                &session_id,
                request_id,
                &request_debug,
                &execute.rop_buffer,
                cached_rop_buffer.unwrap_or_default(),
                &session,
                post_hierarchy_observation,
            );
            let response_debug = summarize_response_rop_buffer(
                execute_success_rop_buffer(&cached.response_body).unwrap_or_default(),
                &request_debug.ids,
            );
            session.record_last_successful_execute_context(
                format!(
                    "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=true",
                    cached.request_rop_ids,
                    cached.response_rop_ids,
                    cached.response_rop_results,
                    cached.response_rop_buffer_bytes
                ),
                request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
            );
            log_post_common_views_handoff_execute_response(
                endpoint,
                principal,
                headers,
                &session_id,
                request_id,
                &session,
                &request_debug,
                &response_debug,
                cached.response_body.len(),
                true,
            );
            store_session(session_id.clone(), session);
            return mapi_response_with_cookies(
                "Execute",
                request_id,
                0,
                cached.response_body,
                session_context_cookies(endpoint, &session_id, false),
            );
        }
        store_session(session_id.clone(), session);
        return execute_failure_response(
            request_id,
            12,
            "reused MAPI Execute request id with a different ROP payload",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }

    if rop_buffer_has_no_requests(&execute.rop_buffer)
        || rop_buffer_is_store_independent_logon(&execute.rop_buffer)
        || rop_buffer_is_store_independent_release_only(&execute.rop_buffer)
        || rop_buffer_is_store_independent_special_folder_getprops_probe(
            &execute.rop_buffer,
            &session,
        )
    {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mailboxes = snapshot.mailboxes();
        let emails = snapshot.emails();
        log_execute_dispatch_start_debug(
            endpoint,
            principal,
            headers,
            request_id,
            mailboxes.len(),
            emails.len(),
        );
        let rop_buffer = execute_rops(
            store,
            principal,
            request_id,
            &mut session,
            &mailboxes,
            &emails,
            &snapshot,
            validator,
            &execute.rop_buffer,
            request_debug.all_release,
            request_debug.handle_count,
            &request_debug.handle_table_summary,
            &request_debug.ids_csv,
            &request_debug.names_csv,
            &request_debug.non_release_rops,
        )
        .await;
        let post_hierarchy_observation =
            if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                session.record_execute_after_hierarchy_completion(
                    &request_debug.ids,
                    &request_debug.names_csv,
                )
            } else {
                PostHierarchyExecuteObservation::default()
            };
        log_execute_rop_debug(
            endpoint,
            principal,
            headers,
            &session_id,
            request_id,
            &request_debug,
            &execute.rop_buffer,
            &rop_buffer,
            &session,
            post_hierarchy_observation,
        );
        let rop_buffer = apply_execute_max_rop_out(
            request_id,
            &execute.rop_buffer,
            rop_buffer,
            execute.max_rop_out,
        );
        let response_body = execute_success_body(rop_buffer, Vec::new());
        let response_debug = summarize_response_rop_buffer(
            execute_success_rop_buffer(&response_body).unwrap_or_default(),
            &request_debug.ids,
        );
        session.record_last_successful_execute_context(
            format!(
                "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=false",
                request_debug.names_csv,
                response_debug.names_csv,
                response_debug.results_csv,
                response_debug.response_payload_bytes
            ),
            request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
        );
        log_post_common_views_handoff_execute_response(
            endpoint,
            principal,
            headers,
            &session_id,
            request_id,
            &session,
            &request_debug,
            &response_debug,
            response_body.len(),
            false,
        );
        cache_execute_response(
            &mut session,
            request_id,
            rop_fingerprint,
            &response_body,
            request_debug.ids_csv.clone(),
            response_debug.ids_csv,
            response_debug.results_csv,
            response_debug.response_payload_bytes,
        );
        store_session(session_id.clone(), session);
        return mapi_response_with_cookies(
            "Execute",
            request_id,
            0,
            response_body,
            session_context_cookies(endpoint, &session_id, false),
        );
    }

    let access_plan = plan_mapi_store_access(&session, &execute.rop_buffer);
    log_execute_store_access_debug(endpoint, principal, headers, request_id, &access_plan);
    let snapshot = match load_mapi_store_for_access_plan(
        store,
        principal.account_id,
        &access_plan,
        500,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(fallback_plan) = hierarchy_sync_selective_fallback_plan(&execute.rop_buffer)
            {
                tracing::warn!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    full_snapshot_error = %format!("{error:#}"),
                    "rca debug mapi full snapshot fallback to hierarchy store view"
                );
                match load_mapi_store_for_access_plan(
                    store,
                    principal.account_id,
                    &fallback_plan,
                    500,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(fallback_error) => {
                        store_session(session_id.clone(), session);
                        return execute_failure_response(
                            request_id,
                            4,
                            &format!(
                                "failed to load MAPI mail store view: {error:#}; fallback failed: {fallback_error:#}"
                            ),
                            Some(session_cookie(endpoint, &session_id, false)),
                        );
                    }
                }
            } else {
                store_session(session_id.clone(), session);
                return execute_failure_response(
                    request_id,
                    4,
                    &format!("failed to load MAPI mail store view: {error:#}"),
                    Some(session_cookie(endpoint, &session_id, false)),
                );
            }
        }
    };
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    log_execute_dispatch_start_debug(
        endpoint,
        principal,
        headers,
        request_id,
        mailboxes.len(),
        emails.len(),
    );
    let rop_buffer = execute_rops(
        store,
        principal,
        request_id,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        validator,
        &execute.rop_buffer,
        request_debug.all_release,
        request_debug.handle_count,
        &request_debug.handle_table_summary,
        &request_debug.ids_csv,
        &request_debug.names_csv,
        &request_debug.non_release_rops,
    )
    .await;
    let post_hierarchy_observation = if endpoint == MapiEndpoint::Emsmdb
        && hierarchy_completed_before_execute
    {
        session
            .record_execute_after_hierarchy_completion(&request_debug.ids, &request_debug.names_csv)
    } else {
        PostHierarchyExecuteObservation::default()
    };
    log_execute_rop_debug(
        endpoint,
        principal,
        headers,
        &session_id,
        request_id,
        &request_debug,
        &execute.rop_buffer,
        &rop_buffer,
        &session,
        post_hierarchy_observation,
    );
    let rop_buffer = apply_execute_max_rop_out(
        request_id,
        &execute.rop_buffer,
        rop_buffer,
        execute.max_rop_out,
    );
    let response_body = execute_success_body(rop_buffer, Vec::new());
    let response_debug = summarize_response_rop_buffer(
        execute_success_rop_buffer(&response_body).unwrap_or_default(),
        &request_debug.ids,
    );
    session.record_last_successful_execute_context(
        format!(
            "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=false",
            request_debug.names_csv,
            response_debug.names_csv,
            response_debug.results_csv,
            response_debug.response_payload_bytes
        ),
        request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
    );
    log_post_common_views_handoff_execute_response(
        endpoint,
        principal,
        headers,
        &session_id,
        request_id,
        &session,
        &request_debug,
        &response_debug,
        response_body.len(),
        false,
    );
    cache_execute_response(
        &mut session,
        request_id,
        rop_fingerprint,
        &response_body,
        request_debug.ids_csv.clone(),
        response_debug.ids_csv,
        response_debug.results_csv,
        response_debug.response_payload_bytes,
    );
    store_session(session_id.clone(), session);
    mapi_response_with_cookies(
        "Execute",
        request_id,
        0,
        response_body,
        session_context_cookies(endpoint, &session_id, false),
    )
}

fn log_post_common_views_handoff_execute_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    session_id: &str,
    request_id: &str,
    session: &MapiSession,
    request: &RopRequestDebugSummary,
    response: &RopResponseDebugSummary,
    response_body_bytes: usize,
    cached_execute_response: bool,
) {
    if endpoint != MapiEndpoint::Emsmdb {
        return;
    }
    let state = &session.post_hierarchy_actions;
    if state.last_common_views_inbox_shortcut_context.is_empty()
        || state.inbox_associated_contents_table_observed
        || state.inbox_normal_contents_table_observed
    {
        return;
    }

    let notification_registered = !state
        .last_inbox_notification_registration_context
        .is_empty();
    let handoff_phase = if notification_registered {
        "post_common_views_notification_handoff"
    } else {
        "post_common_views_inbox_handoff"
    };
    let next_expected_client_step = if notification_registered {
        "notification_wait_or_open_inbox_associated_or_normal_contents_table"
    } else {
        "open_inbox_or_register_notification"
    };
    let cookie_debug = request_cookie_transport_debug(endpoint, headers);
    let session_cookie_debug = cookie_value_debug(Some(session_id));
    let request_sequence_cookie_matches =
        request_sequence_cookie_matches(endpoint, headers, session_id);
    let notification_subscription_count = session
        .handles
        .values()
        .filter(|object| matches!(object, MapiObject::NotificationSubscription { .. }))
        .count();
    let post_handoff_context = format_inbox_post_fai_handoff_context(state);
    let live_handle_summaries = format_live_handle_debug_summary(session);

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        handoff_phase = handoff_phase,
        request_rop_names = %request.names_csv,
        response_rop_names = %response.names_csv,
        response_rop_results = %response.results_csv,
        response_body_bytes = response_body_bytes,
        cached_execute_response = cached_execute_response,
        selected_context_hash = %cookie_debug.selected_context_hash,
        selected_sequence_hash = %cookie_debug.selected_sequence_hash,
        session_id_hash = %session_cookie_debug.hash,
        request_sequence_cookie_matches = request_sequence_cookie_matches,
        notification_subscription_count = notification_subscription_count,
        post_handoff_context = %post_handoff_context,
        live_handle_summaries = %live_handle_summaries,
        next_expected_client_step = next_expected_client_step,
        "rca debug mapi post common views execute response handoff transport"
    );

    let tenant_id = principal.tenant_id.to_string();
    let account_id = principal.account_id.to_string();
    write_outlook_trace(&OutlookTraceEvent {
        component: "mapi",
        endpoint: "emsmdb",
        session_key: session_id,
        direction: OutlookTraceDirection::Outbound,
        phase: "ExecutePostCommonViewsHandoff",
        remote_peer: None,
        tenant_id: Some(&tenant_id),
        account: Some(&principal.email),
        status: Some(200),
        metadata: vec![
            ("account_id", account_id),
            ("mapi_request_id", request_id.to_string()),
            ("handoff_phase", handoff_phase.to_string()),
            ("request_rop_ids", request.ids_csv.clone()),
            ("request_rop_names", request.names_csv.clone()),
            ("response_rop_ids", response.ids_csv.clone()),
            ("response_rop_names", response.names_csv.clone()),
            ("response_rop_results", response.results_csv.clone()),
            ("response_body_bytes", response_body_bytes.to_string()),
            (
                "cached_execute_response",
                cached_execute_response.to_string(),
            ),
            (
                "cookie_header_count",
                cookie_debug.cookie_header_count.to_string(),
            ),
            (
                "mapi_context_candidate_count",
                cookie_debug.context_candidate_count.to_string(),
            ),
            (
                "mapi_sequence_candidate_count",
                cookie_debug.sequence_candidate_count.to_string(),
            ),
            ("selected_context_hash", cookie_debug.selected_context_hash),
            (
                "selected_sequence_hash",
                cookie_debug.selected_sequence_hash,
            ),
            ("session_id_hash", session_cookie_debug.hash),
            (
                "request_sequence_cookie_matches",
                request_sequence_cookie_matches.to_string(),
            ),
            (
                "notification_subscription_count",
                notification_subscription_count.to_string(),
            ),
            ("post_handoff_context", post_handoff_context),
            ("live_handle_summaries", live_handle_summaries),
            (
                "next_expected_client_step",
                next_expected_client_step.to_string(),
            ),
        ],
        payload: None,
    });
}

pub(in crate::mapi) const MAX_ROP_DEBUG_ENTRIES: usize = 32;

pub(in crate::mapi) async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    rop_buffer: &[u8],
    request_all_rops_are_release: bool,
    request_handle_count: usize,
    request_handle_table_summary: &str,
    request_rop_ids: &str,
    request_rop_names: &str,
    request_non_release_rops: &str,
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let (requests, mut handle_slots, extended) = match parse_execute_rop_dispatch_input(rop_buffer)
    {
        Ok(input) => input,
        Err(response) => return response,
    };
    if let Some(max_input_handle) = handle_slots
        .iter()
        .copied()
        .filter(|handle| *handle != u32::MAX)
        .max()
    {
        session.next_handle = session.next_handle.max(max_input_handle.saturating_add(1));
    }

    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    let mut response_handle_indexes = Vec::new();
    let mut response_handle_slots = handle_slots.clone();
    let mut post_hierarchy_release_events = Vec::new();
    let mut same_execute_released_handles = HashSet::new();
    let mut created_emails: Vec<JmapEmail> = Vec::new();
    let mut echo_input_handle_table = false;
    let mut request_has_release = false;
    record_execute_stream_batch_observation(
        principal,
        request_id,
        request_rop_names,
        request_handle_table_summary,
        session,
    );
    while cursor.remaining() > 0 {
        let Some(request) = read_next_execute_rop_request(&mut cursor, &mut responses) else {
            break;
        };
        let typed_request = request.typed();
        let mut completed_hierarchy_sync = None;
        let mut content_sync_configure_observed = false;
        let response_len_before = responses.len();
        if let Some(response) = unknown_property_wire_type_response(principal, &request) {
            responses.extend_from_slice(&response);
            response_handle_indexes.push(request.response_handle_index());
            response_handle_slots = handle_slots.clone();
            break;
        }
        match RopId::from_u8(typed_request.rop_id()) {
            Some(rop_id) if is_release_dispatch_rop(rop_id) => {
                request_has_release = true;
                echo_input_handle_table |= append_release_dispatch_response(
                    store,
                    principal,
                    request_id,
                    request_rop_names,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut same_execute_released_handles,
                    &mut post_hierarchy_release_events,
                )
                .await;
                if !responses.is_empty() {
                    response_handle_slots = handle_slots.clone();
                }
            }
            Some(rop_id) if is_folder_open_rop(rop_id) => {
                append_folder_open_dispatch_response(
                    store,
                    principal,
                    request_id,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &same_execute_released_handles,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_message_dispatch_rop(rop_id) => {
                append_message_dispatch_response(
                    store,
                    principal,
                    request_id,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                    &mut created_emails,
                )
                .await;
            }
            Some(rop_id) if is_table_open_rop(rop_id) => {
                append_table_open_dispatch_response(
                    store,
                    principal,
                    request_id,
                    request_rop_names,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_property_dispatch_rop(rop_id) => {
                let flow = append_property_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    request_id,
                    mailboxes,
                    emails,
                    &created_emails,
                    snapshot,
                    &mut responses,
                )
                .await;
                echo_input_handle_table |= flow.echo_input_handle_table;
                if flow.stop_batch {
                    break;
                }
            }
            Some(rop_id) if is_recipient_rop(rop_id) => {
                append_recipient_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                )
                .await;
            }
            Some(rop_id) if is_message_state_rop(rop_id) => {
                append_message_state_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                )
                .await;
            }
            Some(rop_id) if is_table_control_rop(rop_id, session, &handle_slots, &request) => {
                if matches!(
                    append_table_control_dispatch_response(
                        principal,
                        request_id,
                        &request_rop_names,
                        session,
                        &handle_slots,
                        &request,
                        mailboxes,
                        emails,
                        snapshot,
                        &mut responses,
                    ),
                    TableControlFlow::StopBatch
                ) {
                    break;
                }
            }
            Some(rop_id) if is_folder_dispatch_rop(rop_id) => {
                append_folder_dispatch_response(
                    store,
                    principal,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_attachment_rop(rop_id) => {
                append_attachment_response(
                    store,
                    principal,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    validator,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_stream_dispatch_rop(rop_id) => {
                append_stream_dispatch_response(
                    store,
                    principal,
                    request_id,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_submission_dispatch_rop(rop_id) => {
                append_submission_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    &mut responses,
                    &mut created_emails,
                )
                .await;
            }
            Some(rop_id) if is_receive_folder_rop(rop_id) => {
                echo_input_handle_table |= append_receive_folder_dispatch_response(
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    &mut responses,
                );
            }
            Some(rop_id) if is_search_criteria_rop(rop_id) => {
                append_search_criteria_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    request_id,
                    mailboxes,
                    snapshot,
                    &mut responses,
                )
                .await;
            }
            Some(rop_id) if is_sync_transfer_rop(rop_id) => {
                if append_sync_transfer_dispatch_response(
                    store,
                    principal,
                    request_id,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                    &mut completed_hierarchy_sync,
                    &mut content_sync_configure_observed,
                )
                .await
                {
                    break;
                }
            }
            Some(rop_id) if is_sync_import_rop(rop_id) => {
                echo_input_handle_table |= append_sync_import_dispatch_response(
                    store,
                    principal,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_object_id_conversion_rop(rop_id) => {
                append_object_id_conversion_response(
                    principal,
                    &request,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                );
            }
            Some(rop_id) if is_public_folder_metadata_rop(rop_id) => {
                append_public_folder_metadata_dispatch_response(
                    store,
                    principal,
                    session,
                    &handle_slots,
                    &request,
                    snapshot,
                    &mut responses,
                )
                .await;
            }
            Some(rop_id) if is_logon_dispatch_rop(rop_id) => {
                echo_input_handle_table |= append_logon_dispatch_response(
                    session,
                    &mut handle_slots,
                    &request,
                    &typed_request,
                    principal,
                    request_id,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                );
            }
            Some(rop_id) if is_named_property_rop(rop_id) => {
                echo_input_handle_table |= append_named_property_dispatch_response(
                    store,
                    principal,
                    request_id,
                    session,
                    &handle_slots,
                    &request,
                    &mut responses,
                )
                .await;
            }
            Some(rop_id) if is_notification_dispatch_rop(rop_id) => {
                append_notification_dispatch_response(
                    store,
                    principal,
                    request_id,
                    &request_rop_names,
                    session,
                    &mut handle_slots,
                    &request,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_permissions_dispatch_rop(rop_id) => {
                append_permissions_dispatch_response(
                    store,
                    principal,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_rules_dispatch_rop(rop_id) => {
                append_rules_dispatch_response(
                    store,
                    principal,
                    session,
                    &mut handle_slots,
                    &request,
                    mailboxes,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_status_or_bookmark_rop(rop_id) => {
                append_status_or_bookmark_dispatch_response(
                    session,
                    &handle_slots,
                    &request,
                    &mut responses,
                );
            }
            Some(rop_id) => {
                append_unsupported_known_dispatch_response(rop_id, &request, &mut responses);
            }
            None => {
                append_unsupported_unknown_dispatch_response(&request, &mut responses);
                response_handle_indexes.push(request.response_handle_index());
                response_handle_slots = handle_slots.clone();
                break;
            }
        }
        if responses.len() != response_len_before {
            response_handle_indexes.push(request.response_handle_index());
            response_handle_slots = handle_slots.clone();
        }
        record_execute_sync_observations(
            session,
            completed_hierarchy_sync,
            content_sync_configure_observed,
        );
        if typed_request.unsupported_is_terminal() {
            break;
        }
    }
    log_post_hierarchy_release_events(
        principal,
        request_id,
        request_rop_ids,
        request_rop_names,
        request_non_release_rops,
        request_all_rops_are_release,
        request_handle_count,
        request_handle_table_summary,
        session,
        &post_hierarchy_release_events,
        &responses,
    );
    let final_handle_slots = if request_has_release && !responses.is_empty() {
        &response_handle_slots
    } else {
        &handle_slots
    };
    finalize_execute_rop_buffer(
        responses,
        final_handle_slots,
        &output_handles,
        &response_handle_indexes,
        echo_input_handle_table,
        request_has_release,
        extended,
    )
}

#[cfg(test)]
mod tests;
