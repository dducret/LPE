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
    MapiCustomPropertyObjectKind, MapiCustomPropertyValue, MapiFolderProfilePropertyValue,
    MapiIdentityObjectKind, MapiIdentityRequest, MapiSyncChangeSet, MapiSyncCheckpoint,
    UpsertMapiAssociatedConfigInput, UpsertMapiNavigationShortcutInput,
};
use lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, JmapEmail, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, PublicFolderPermissionInput, SearchFolderDefinition,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
    UpsertSearchFolderInput,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

mod associated_config;
mod attachments;
mod contacts;
mod default_folders;
mod diagnostics;
mod execute;
mod folders;
mod logon;
mod messages;
mod object_ids;
mod property_tags;
mod public_folders;
mod recipients;
mod recoverable_items;
mod rules;
mod search_folders;
mod submission;
mod sync_import;
mod table_diagnostics;
mod table_validation;
mod tables;

use associated_config::*;
use attachments::*;
use contacts::*;
use default_folders::*;
pub(in crate::mapi) use diagnostics::*;
pub(in crate::mapi) use execute::*;
use folders::*;
use logon::*;
use messages::*;
use object_ids::*;
use property_tags::*;
use public_folders::*;
use recipients::*;
use recoverable_items::*;
use rules::*;
use search_folders::*;
use submission::*;
use sync_import::*;
use table_diagnostics::*;
use table_validation::*;
use tables::*;

const HIERARCHY_SYNC_CURSOR_VERSION: u64 = 2;

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

fn current_mapi_filetime() -> u64 {
    const FILETIME_UNIX_EPOCH_SECONDS: u64 = 11_644_473_600;
    const FILETIME_TICKS_PER_SECOND: u64 = 10_000_000;

    let unix_ticks = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| {
            duration
                .as_secs()
                .saturating_mul(FILETIME_TICKS_PER_SECOND)
                .saturating_add(u64::from(duration.subsec_nanos() / 100))
        })
        .unwrap_or(0);
    FILETIME_UNIX_EPOCH_SECONDS
        .saturating_mul(FILETIME_TICKS_PER_SECOND)
        .saturating_add(unix_ticks)
}

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
                    session.record_execute_after_hierarchy_completion(&request_debug.ids)
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
                session.record_execute_after_hierarchy_completion(&request_debug.ids)
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
    let post_hierarchy_observation =
        if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
            session.record_execute_after_hierarchy_completion(&request_debug.ids)
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

pub(in crate::mapi) const MAX_ROP_DEBUG_ENTRIES: usize = 32;

fn apply_outlook_smart_input_variant_before_query_rows(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    request_rop_names: &str,
) -> Option<String> {
    if session.outlook_smart_input_variant != "fai_cursor_reset_before_query_rows" {
        return None;
    }
    let input_index = request.input_handle_index().unwrap_or(0);
    let handle = input_handle(handle_slots, request);
    let Some(handle_value) = handle else {
        return None;
    };
    let Some(mut object) = session.handles.remove(&handle_value) else {
        return None;
    };
    let applied = match &mut object {
        MapiObject::ContentsTable {
            folder_id,
            associated,
            position,
            ..
        } if *folder_id == INBOX_FOLDER_ID && *associated => {
            let previous_position = *position;
            *position = 0;
            Some((*folder_id, previous_position))
        }
        _ => None,
    };
    session.handles.insert(handle_value, object);
    let Some((folder_id, previous_position)) = applied else {
        return None;
    };
    session.outlook_smart_input_variant_applied = true;
    Some(format!(
        "variant=fai_cursor_reset_before_query_rows;request_id={request_id};request_rops={request_rop_names};input_index={input_index};handle={};folder=0x{folder_id:016x};associated=true;previous_position={previous_position};new_position=0",
        format_optional_debug_handle(handle)
    ))
}

async fn apply_supported_object_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: &MapiObject,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let (canonical_values, custom_values) = split_object_property_values(object, values);
    if let Some(folder_id) = object.folder_id() {
        if !snapshot
            .folder_access_for_principal(folder_id, principal.account_id)
            .map(|access| access.may_write)
            .unwrap_or(true)
        {
            return Err(anyhow!(
                "MAPI object mutation denied by canonical folder rights"
            ));
        }
    }
    if !canonical_values.is_empty() {
        match object {
            MapiObject::Message {
                folder_id,
                message_id,
                ..
            } => {
                apply_canonical_message_property_values(
                    store,
                    principal,
                    *folder_id,
                    *message_id,
                    canonical_values,
                    mailboxes,
                    emails,
                )
                .await?;
            }
            MapiObject::Contact {
                folder_id,
                contact_id,
            } => {
                apply_canonical_contact_property_values(
                    store,
                    principal,
                    *folder_id,
                    *contact_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Event {
                folder_id,
                event_id,
            } => {
                apply_canonical_event_property_values(
                    store,
                    principal,
                    *folder_id,
                    *event_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Task { folder_id, task_id } => {
                apply_canonical_task_property_values(
                    store,
                    principal,
                    *folder_id,
                    *task_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Note { folder_id, note_id } => {
                apply_canonical_note_property_values(
                    store,
                    principal,
                    *folder_id,
                    *note_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::JournalEntry {
                folder_id,
                journal_entry_id,
            } => {
                apply_canonical_journal_entry_property_values(
                    store,
                    principal,
                    *folder_id,
                    *journal_entry_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::ConversationAction {
                folder_id,
                conversation_action_id,
            } => {
                let Some(existing) = snapshot
                    .conversation_action_message_for_id(*conversation_action_id)
                    .filter(|message| message.folder_id == *folder_id)
                else {
                    return Err(anyhow!("canonical MAPI conversation action was not found"));
                };
                let mut properties = conversation_action_properties(&existing.action);
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let action = conversation_action_from_mapi_properties(&properties);
                let move_target_mailbox_id =
                    conversation_action_target_mailbox_id(&action, mailboxes);
                let input = lpe_storage::UpsertConversationActionInput {
                    account_id: principal.account_id,
                    conversation_id: action.conversation_id,
                    subject: action.subject,
                    categories_json: action.categories_json,
                    move_folder_entry_id: action.move_folder_entry_id,
                    move_store_entry_id: action.move_store_entry_id,
                    move_target_mailbox_id,
                    max_delivery_time: action.max_delivery_time,
                    last_applied_time: action.last_applied_time,
                    version: Some(action.version),
                    processed: Some(action.processed),
                };
                let saved = store.upsert_conversation_action(input).await?;
                apply_conversation_action_to_existing_messages(
                    store, principal, &saved, mailboxes, emails,
                )
                .await?;
            }
            MapiObject::NavigationShortcut {
                folder_id,
                shortcut_id,
            } => {
                let Some(existing) = snapshot
                    .navigation_shortcut_message_for_id(*shortcut_id)
                    .filter(|message| message.folder_id == *folder_id)
                else {
                    return Err(anyhow!("canonical MAPI navigation shortcut was not found"));
                };
                let mut properties = HashMap::new();
                for tag in [
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_WLINK_ENTRY_ID,
                    PID_TAG_WLINK_SAVE_STAMP,
                    PID_TAG_WLINK_TYPE,
                    PID_TAG_WLINK_FLAGS,
                    PID_TAG_WLINK_SECTION,
                    PID_TAG_WLINK_ORDINAL,
                ] {
                    if let Some(value) =
                        navigation_shortcut_property_value(&existing, principal.account_id, tag)
                    {
                        properties.insert(tag, value);
                    }
                }
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let shortcut = navigation_shortcut_from_mapi_properties(
                    principal.account_id,
                    Some(existing.canonical_id),
                    &properties,
                );
                store
                    .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                        id: Some(shortcut.canonical_id),
                        account_id: principal.account_id,
                        subject: shortcut.subject,
                        target_folder_id: shortcut.target_folder_id,
                        shortcut_type: shortcut.shortcut_type,
                        flags: shortcut.flags,
                        save_stamp: shortcut.save_stamp,
                        section: shortcut.section,
                        ordinal: shortcut.ordinal,
                        group_header_id: shortcut.group_header_id,
                        group_name: shortcut.group_name,
                    })
                    .await?;
            }
            MapiObject::AssociatedConfig {
                folder_id,
                config_id,
                saved_message,
            } => {
                let Some(existing) = associated_config_message_for_mutation(
                    snapshot,
                    *folder_id,
                    *config_id,
                    saved_message.as_ref(),
                ) else {
                    return Err(anyhow!("MAPI associated config message was not found"));
                };
                let mut properties = associated_config_mutation_base_properties(&existing);
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let (message_class, subject) = associated_config_class_and_subject(&properties);
                store
                    .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
                        id: Some(existing.canonical_id),
                        account_id: principal.account_id,
                        folder_id: *folder_id,
                        message_class,
                        subject,
                        properties_json: mapi_properties_to_json(&properties),
                    })
                    .await?;
            }
            MapiObject::PublicFolderItem {
                folder_id, item_id, ..
            } => {
                apply_canonical_public_folder_item_property_values(
                    store,
                    principal,
                    *folder_id,
                    *item_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::DelegateFreeBusyMessage { .. } | MapiObject::RecoverableItem { .. } => {}
            _ => return Err(anyhow!("MAPI object does not support property mutation")),
        }
    }
    if custom_values.is_empty() {
        return Ok(());
    }
    let (object_kind, canonical_id) =
        custom_property_object_identity(Some(object), mailboxes, emails, snapshot)
            .ok_or_else(|| anyhow!("canonical MAPI object was not found"))?;
    upsert_custom_property_values(store, principal, object_kind, canonical_id, custom_values).await
}

async fn apply_canonical_public_folder_item_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    item_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let Some(item) = snapshot.public_folder_item_for_id(folder_id, item_id) else {
        return Err(anyhow!("canonical public-folder item was not found"));
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String(item.item.message_class.clone()),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String(item.item.subject.clone()),
    );
    properties.insert(
        PID_TAG_NORMALIZED_SUBJECT_W,
        MapiValue::String(item.item.subject.clone()),
    );
    properties.insert(
        PID_TAG_BODY_W,
        MapiValue::String(item.item.body_text.clone()),
    );
    if let Some(html) = &item.item.body_html_sanitized {
        properties.insert(PID_TAG_BODY_HTML_W, MapiValue::String(html.clone()));
        properties.insert(
            PID_TAG_HTML_BINARY,
            MapiValue::Binary(html.as_bytes().to_vec()),
        );
    }
    apply_mapi_property_values_to_map(&mut properties, values);
    store
        .upsert_public_folder_item(
            UpsertPublicFolderItemInput {
                id: Some(item.item.id),
                account_id: principal.account_id,
                public_folder_id: item.item.public_folder_id,
                item_kind: item.item.item_kind.clone(),
                message_class: optional_pending_text_property(
                    &properties,
                    &[PID_TAG_MESSAGE_CLASS_W],
                )
                .unwrap_or_else(|| "IPM.Post".to_string()),
                subject: pending_text_property(
                    &properties,
                    &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
                ),
                body_text: pending_text_property(&properties, &[PID_TAG_BODY_W]),
                body_html_sanitized: pending_html_property(&properties),
                source_payload_json: item.item.source_payload_json.clone(),
            },
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-update-public-folder-item".to_string(),
                subject: format!("public-folder-item:{}", item.item.id),
            },
        )
        .await?;
    Ok(())
}

async fn persist_profile_folder_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Result<()>
where
    S: ExchangeStore,
{
    let folder_profile_values = values
        .iter()
        .filter_map(|(tag, value)| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if storage_tag != PID_TAG_EXTENDED_FOLDER_FLAGS {
                return None;
            }
            let MapiValue::Binary(bytes) = value else {
                return None;
            };
            Some(MapiFolderProfilePropertyValue {
                folder_id,
                property_tag: storage_tag,
                property_type: (PID_TAG_EXTENDED_FOLDER_FLAGS & 0xffff) as u16,
                property_value: bytes.clone(),
            })
        })
        .collect::<Vec<_>>();
    if !folder_profile_values.is_empty() {
        store
            .upsert_mapi_folder_profile_property_values(
                principal.account_id,
                &folder_profile_values,
            )
            .await?;
    }
    if folder_id != IPM_SUBTREE_FOLDER_ID {
        return Ok(());
    }
    for (tag, value) in values {
        if canonical_property_storage_tag(*tag) == PID_TAG_OST_OSTID {
            if let MapiValue::Binary(ost_id) = value {
                store
                    .store_mapi_ipm_subtree_ost_id(principal.account_id, ost_id)
                    .await?;
            }
        }
    }
    Ok(())
}

fn split_custom_property_values(
    values: Vec<(u32, MapiValue)>,
) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>) {
    values
        .into_iter()
        .partition(|(tag, _)| !is_custom_property_tag(*tag))
}

fn split_object_property_values(
    object: &MapiObject,
    values: Vec<(u32, MapiValue)>,
) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>) {
    if !matches!(object, MapiObject::AssociatedConfig { .. }) {
        return split_custom_property_values(values);
    }
    (values, Vec::new())
}

fn apply_mapi_property_values_to_map(
    properties: &mut HashMap<u32, MapiValue>,
    values: Vec<(u32, MapiValue)>,
) {
    properties.extend(
        values
            .into_iter()
            .map(|(tag, value)| (canonical_property_storage_tag(tag), value)),
    );
}

fn conversation_action_properties(
    action: &lpe_storage::ConversationAction,
) -> HashMap<u32, MapiValue> {
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_CONVERSATION_INDEX,
        MapiValue::Binary(conversation_index_for_uuid(action.conversation_id)),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String(conversation_action_subject(action)),
    );
    if let Some(value) = &action.move_folder_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.move_store_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.max_delivery_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    if let Some(value) = &action.last_applied_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    properties.insert(
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        MapiValue::I32(action.version),
    );
    properties.insert(
        PID_LID_CONVERSATION_PROCESSED_TAG,
        MapiValue::I32(action.processed),
    );
    properties.insert(
        PID_NAME_KEYWORDS_TAG,
        MapiValue::MultiString(
            serde_json::from_str::<Vec<String>>(&action.categories_json).unwrap_or_default(),
        ),
    );
    properties
}

async fn apply_conversation_action_to_existing_messages<S>(
    store: &S,
    principal: &AccountPrincipal,
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let categories = serde_json::from_str::<Vec<String>>(&action.categories_json)
        .unwrap_or_default()
        .into_iter()
        .map(|category| category.trim().to_string())
        .filter(|category| !category.is_empty())
        .collect::<Vec<_>>();
    let target_mailbox = if action.move_store_entry_id.is_some() {
        None
    } else {
        conversation_action_target_mailbox(action, mailboxes)
    };
    for email in emails
        .iter()
        .filter(|email| email.thread_id == action.conversation_id)
        .filter(|email| email.mailbox_role != "sent")
        .filter(|email| {
            action
                .max_delivery_time
                .as_deref()
                .map(|max_delivery| email.received_at.as_str() > max_delivery)
                .unwrap_or(true)
        })
    {
        if !categories.is_empty() && email.categories != categories {
            store
                .update_jmap_email_followup_flags(
                    principal.account_id,
                    email.id,
                    lpe_storage::JmapEmailFollowupUpdate {
                        categories: Some(categories.clone()),
                        ..Default::default()
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-conversation-action-categorize".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await?;
        }
        let Some(target_mailbox) = target_mailbox else {
            continue;
        };
        if email.mailbox_id == target_mailbox.id {
            continue;
        }
        store
            .move_jmap_email_from_mailbox(
                principal.account_id,
                email.mailbox_id,
                email.id,
                target_mailbox.id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-conversation-action-move".to_string(),
                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                },
            )
            .await?;
    }
    Ok(())
}

async fn apply_conversation_actions_to_new_message<S>(
    store: &S,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    email: &JmapEmail,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    for message in snapshot
        .conversation_action_messages()
        .iter()
        .filter(|message| message.action.conversation_id == email.thread_id)
    {
        apply_conversation_action_to_existing_messages(
            store,
            principal,
            &message.action,
            mailboxes,
            std::slice::from_ref(email),
        )
        .await?;
    }
    Ok(())
}

fn conversation_action_target_mailbox<'a>(
    action: &lpe_storage::ConversationAction,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    if action.move_store_entry_id.is_some() {
        return None;
    }
    if let Some(mailbox_id) = action.move_target_mailbox_id {
        return mailboxes.iter().find(|mailbox| mailbox.id == mailbox_id);
    }
    match action.move_folder_entry_id.as_deref() {
        Some([]) => mailboxes.iter().find(|mailbox| mailbox.role == "trash"),
        Some(entry_id) => {
            let folder_id = crate::mapi::identity::object_id_from_folder_entry_id(entry_id)?;
            folder_row_for_id(folder_id, mailboxes)
        }
        None => None,
    }
}

fn conversation_action_target_mailbox_id(
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    conversation_action_target_mailbox(action, mailboxes).map(|mailbox| mailbox.id)
}

async fn delete_conversation_action_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    conversation_action_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let existing = snapshot
        .conversation_action_message_for_id(conversation_action_id)
        .filter(|message| message.folder_id == folder_id)
        .ok_or_else(|| anyhow!("canonical MAPI conversation action was not found"))?;
    let mut properties = conversation_action_properties(&existing.action);
    for tag in property_tags {
        properties.remove(tag);
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    let action = conversation_action_from_mapi_properties(&properties);
    let move_target_mailbox_id = conversation_action_target_mailbox_id(&action, mailboxes);
    let saved = store
        .upsert_conversation_action(lpe_storage::UpsertConversationActionInput {
            account_id: principal.account_id,
            conversation_id: action.conversation_id,
            subject: action.subject,
            categories_json: action.categories_json,
            move_folder_entry_id: action.move_folder_entry_id,
            move_store_entry_id: action.move_store_entry_id,
            move_target_mailbox_id,
            max_delivery_time: action.max_delivery_time,
            last_applied_time: action.last_applied_time,
            version: Some(action.version),
            processed: Some(action.processed),
        })
        .await?;
    apply_conversation_action_to_existing_messages(store, principal, &saved, mailboxes, emails)
        .await
}

fn stage_virtual_conversation_action_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Option<Result<()>> {
    let object = input_object_mut(session, handle_slots, request)?;
    let MapiObject::ConversationAction {
        folder_id,
        conversation_action_id,
    } = object
    else {
        return None;
    };
    if !crate::mapi_store::is_outlook_default_conversation_action_id(*conversation_action_id) {
        return None;
    }
    let Some(message) = snapshot
        .conversation_action_table_message_for_id(*conversation_action_id)
        .filter(|message| message.folder_id == *folder_id)
    else {
        return Some(Err(anyhow!(
            "virtual MAPI conversation action was not found"
        )));
    };
    let folder_id = *folder_id;
    let mut properties = conversation_action_properties(&message.action);
    apply_mapi_property_values_to_map(&mut properties, values);
    *object = MapiObject::PendingConversationAction {
        folder_id,
        properties,
    };
    Some(Ok(()))
}

fn stage_virtual_conversation_action_property_delete(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Option<Result<()>> {
    let object = input_object_mut(session, handle_slots, request)?;
    let MapiObject::ConversationAction {
        folder_id,
        conversation_action_id,
    } = object
    else {
        return None;
    };
    if !crate::mapi_store::is_outlook_default_conversation_action_id(*conversation_action_id) {
        return None;
    }
    let Some(message) = snapshot
        .conversation_action_table_message_for_id(*conversation_action_id)
        .filter(|message| message.folder_id == *folder_id)
    else {
        return Some(Err(anyhow!(
            "virtual MAPI conversation action was not found"
        )));
    };
    let folder_id = *folder_id;
    let mut properties = conversation_action_properties(&message.action);
    for tag in property_tags {
        properties.remove(tag);
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    *object = MapiObject::PendingConversationAction {
        folder_id,
        properties,
    };
    Some(Ok(()))
}

async fn upsert_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiCustomPropertyObjectKind,
    canonical_id: Uuid,
    values: Vec<(u32, MapiValue)>,
) -> Result<()>
where
    S: ExchangeStore,
{
    if values.is_empty() {
        return Ok(());
    }
    let values = values
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            MapiCustomPropertyValue {
                property_tag,
                property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    store
        .upsert_mapi_custom_property_values(
            principal.account_id,
            object_kind,
            canonical_id,
            &values,
        )
        .await
}

async fn upsert_custom_property_values_from_map<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiCustomPropertyObjectKind,
    canonical_id: Uuid,
    properties: &HashMap<u32, MapiValue>,
) -> Result<()>
where
    S: ExchangeStore,
{
    let values = properties
        .iter()
        .filter(|(tag, _value)| is_custom_property_tag(**tag))
        .map(|(tag, value)| (*tag, value.clone()))
        .collect::<Vec<_>>();
    upsert_custom_property_values(store, principal, object_kind, canonical_id, values).await
}

async fn fetch_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<HashMap<u32, Vec<u8>>>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(HashMap::new());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(HashMap::new());
    };
    Ok(store
        .fetch_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value.property_value))
        .collect())
}

async fn copy_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Option<Vec<(usize, u32, u32)>>>
where
    S: ExchangeStore,
{
    if property_tags.is_empty() || !property_tags.iter().copied().all(is_custom_property_tag) {
        return Ok(None);
    }
    let Some((source_kind, source_id)) =
        custom_property_object_identity(source, mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination, mailboxes, emails, snapshot)
    else {
        return Ok(None);
    };
    let source_values = store
        .fetch_mapi_custom_property_values(
            principal.account_id,
            source_kind,
            source_id,
            property_tags,
        )
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value))
        .collect::<HashMap<_, _>>();
    let mut copied_values = Vec::new();
    let mut problems = Vec::new();
    for (index, property_tag) in property_tags.iter().copied().enumerate() {
        if let Some(value) = source_values.get(&property_tag) {
            copied_values.push(MapiCustomPropertyValue {
                property_tag,
                property_type: value.property_type,
                property_value: value.property_value.clone(),
            });
        } else {
            problems.push((index, property_tag, 0x8004_010F));
        }
    }
    if !copied_values.is_empty() {
        store
            .upsert_mapi_custom_property_values(
                principal.account_id,
                destination_kind,
                destination_id,
                &copied_values,
            )
            .await?;
    }
    Ok(Some(problems))
}

async fn copy_all_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    excluded_property_tags: &[u32],
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some((source_kind, source_id)) =
        custom_property_object_identity(source, mailboxes, emails, snapshot)
    else {
        return Ok(false);
    };
    let Some((destination_kind, destination_id)) =
        custom_property_object_identity(destination, mailboxes, emails, snapshot)
    else {
        return Ok(false);
    };
    let excluded = excluded_property_tags
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let values = store
        .fetch_all_mapi_custom_property_values(principal.account_id, source_kind, source_id)
        .await?
        .into_iter()
        .filter(|value| !excluded.contains(&value.property_tag))
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(false);
    }
    store
        .upsert_mapi_custom_property_values(
            principal.account_id,
            destination_kind,
            destination_id,
            &values,
        )
        .await?;
    Ok(true)
}

async fn delete_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<()>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(());
    };
    store
        .delete_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await
}

fn custom_property_object_identity(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(MapiCustomPropertyObjectKind, Uuid)> {
    match object? {
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        } => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or(saved_email.as_ref().map(|saved| &saved.email))
            .map(|email| (MapiCustomPropertyObjectKind::Message, email.id)),
        MapiObject::Contact {
            folder_id,
            contact_id,
        } => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| (MapiCustomPropertyObjectKind::Contact, contact.canonical_id)),
        MapiObject::Event {
            folder_id,
            event_id,
        } if !mapi_calendar_content_items_suppressed(*folder_id, snapshot) => {
            snapshot.event_for_id(*folder_id, *event_id).map(|event| {
                (
                    MapiCustomPropertyObjectKind::CalendarEvent,
                    event.canonical_id,
                )
            })
        }
        MapiObject::Task { folder_id, task_id } => snapshot
            .task_for_id(*folder_id, *task_id)
            .map(|task| (MapiCustomPropertyObjectKind::Task, task.canonical_id)),
        MapiObject::Note { folder_id, note_id } => snapshot
            .note_for_id(*folder_id, *note_id)
            .map(|note| (MapiCustomPropertyObjectKind::Note, note.canonical_id)),
        MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        } => snapshot
            .journal_entry_for_id(*folder_id, *journal_entry_id)
            .map(|entry| {
                (
                    MapiCustomPropertyObjectKind::JournalEntry,
                    entry.canonical_id,
                )
            }),
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| {
                (
                    MapiCustomPropertyObjectKind::Attachment,
                    attachment.canonical_id,
                )
            }),
        MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        } => snapshot
            .public_folder_item_for_id(*folder_id, *item_id)
            .map(|item| (MapiCustomPropertyObjectKind::PublicFolderItem, item.item.id)),
        _ => None,
    }
}

fn is_custom_property_tag(property_tag: u32) -> bool {
    let tag = MapiPropertyTag::new(property_tag);
    tag.property_id() >= FIRST_NAMED_PROPERTY_ID
        && tag.property_type().is_some()
        && !is_canonical_named_property_tag(property_tag)
}

fn is_canonical_named_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_LID_FLAG_REQUEST_W_TAG
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_BUSY_STATUS_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_APPOINTMENT_DURATION_TAG
            | PID_LID_APPOINTMENT_RECUR_TAG
            | PID_LID_APPOINTMENT_SUB_TYPE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_RECURRING_TAG
            | PID_LID_ALL_ATTENDEES_STRING_W_TAG
            | PID_LID_TO_ATTENDEES_STRING_W_TAG
            | PID_LID_CC_ATTENDEES_STRING_W_TAG
            | PID_LID_TIME_ZONE_STRUCT_TAG
            | PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
            | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_NOTE_COLOR_TAG
            | PID_LID_LOG_TYPE_W_TAG
            | PID_LID_COMPANIES_TAG
            | PID_LID_CONTACTS_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_VERSION_TAG
            | PID_LID_CONVERSATION_PROCESSED_TAG
            | PID_NAME_KEYWORDS_TAG
    )
}

fn delegate_freebusy_message_for_open<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<&'a crate::mapi_store::MapiDelegateFreeBusyMessage> {
    (folder_id == FREEBUSY_DATA_FOLDER_ID)
        .then(|| snapshot.delegate_freebusy_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

fn conversation_action_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiConversationActionMessage> {
    (folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID)
        .then(|| snapshot.conversation_action_table_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

fn navigation_shortcut_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiNavigationShortcutMessage> {
    (folder_id == COMMON_VIEWS_FOLDER_ID)
        .then(|| snapshot.navigation_shortcut_table_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

fn common_view_named_view_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage> {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        return snapshot
            .common_view_named_view_message_for_id(message_id)
            .filter(|message| message.folder_id == folder_id);
    }
    folder_local_default_named_view_is_supported(snapshot, folder_id, message_id)
        .then(|| snapshot.default_folder_named_view_message(folder_id, message_id))
        .flatten()
}

fn search_folder_definition_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<SearchFolderDefinition> {
    (folder_id == COMMON_VIEWS_FOLDER_ID)
        .then(|| {
            snapshot.common_views_table_messages().find_map(|message| {
                if let crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(
                    definition,
                ) = message
                {
                    (crate::mapi::identity::mapped_mapi_object_id(&definition.id)
                        == Some(message_id))
                    .then_some(definition)
                } else {
                    None
                }
            })
        })
        .flatten()
}

fn contains_outlook_osc_contact_source_probe(properties: &[MapiNamedProperty]) -> bool {
    properties.iter().any(|property| {
        property.guid == PS_PUBLIC_STRINGS_GUID
            && match &property.kind {
                MapiNamedPropertyKind::Name(name) => name.eq_ignore_ascii_case("OscContactSources"),
                MapiNamedPropertyKind::Lid(lid) => matches!(
                    *lid,
                    PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED
                ),
            }
    })
}

fn cache_named_property_mapping_and_return_property_id(
    session: &mut MapiSession,
    property_id: u16,
    property: MapiNamedProperty,
) -> u16 {
    let property_for_lookup = property.clone();
    session.cache_named_property(property_id, property);
    session
        .property_id_for_name(property_for_lookup, false)
        .unwrap_or(property_id)
}

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
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(rop_parse_error_response(), &[]);
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    let mut handle_slots = match read_handle_table(handle_table) {
        Ok(handle_slots) => handle_slots,
        Err(_) => {
            let response = if extended {
                rop_buffer_with_response_spec(rop_parse_error_response(), &[])
            } else {
                rop_buffer_with_response(rop_parse_error_response(), &[])
            };
            return if extended {
                rpc_header_ext_rop_buffer(response)
            } else {
                response
            };
        }
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
    let mut post_hierarchy_release_events = Vec::new();
    let mut same_execute_released_handles = HashSet::new();
    let mut created_emails: Vec<JmapEmail> = Vec::new();
    let mut echo_input_handle_table = false;
    if request_rop_names == "SetProperties,OpenStream,SetStreamSize,WriteStream,CommitStream" {
        let summary = format!(
            "request_id={request_id};request_rops={request_rop_names};handles={request_handle_table_summary}"
        );
        session.record_outlook_stream_batch_observed(summary.clone());
        session.record_outlook_view_failure_trace_event(format!("stream_batch_observed:{summary}"));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_names = %request_rop_names,
            input_handle_table_summary = %request_handle_table_summary,
            stream_batch_observed = true,
            "rca debug outlook stream batch observed"
        );
    }
    while cursor.remaining() > 0 {
        if cursor.remaining_is_zero_padding() {
            break;
        }
        let request = match read_rop_request(&mut cursor) {
            Ok(request) => request,
            Err(_) => {
                responses.extend_from_slice(&rop_parse_error_response());
                break;
            }
        };
        let typed_request = request.typed();
        let mut completed_hierarchy_sync = None;
        let mut content_sync_configure_observed = false;
        if let Some(response) = unknown_property_wire_type_response(principal, &request) {
            responses.extend_from_slice(&response);
            break;
        }
        match RopId::from_u8(typed_request.rop_id()) {
            Some(RopId::Release) => {
                echo_input_handle_table = true;
                let released_handle = input_handle(&handle_slots, &request);
                let released_object = input_object(session, &handle_slots, &request);
                let released_object_for_stream_persist = released_object.cloned();
                let released_object_kind = mapi_object_debug_kind(released_object);
                let released_folder_id = mapi_object_debug_folder_id(released_object);
                let released_folder_role = released_object
                    .and_then(MapiObject::folder_id)
                    .map(debug_role_for_folder_id)
                    .unwrap_or_default();
                let released_associated_contents_table = matches!(
                    released_object,
                    Some(MapiObject::ContentsTable {
                        associated: true,
                        ..
                    })
                );
                let inbox_related_release_context = format_inbox_related_release_context(
                    released_object,
                    released_handle,
                    &session.post_hierarchy_actions,
                    snapshot,
                );
                let inbox_related_release_context_for_log =
                    inbox_related_release_context.clone().unwrap_or_default();
                let visible_inbox_release_without_query_rows = match released_object {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID
                        && !*associated
                        && !columns.is_empty()
                        && session
                            .post_hierarchy_actions
                            .last_inbox_normal_contents_table_setcolumns_handle
                            == released_handle
                        && session
                            .post_hierarchy_actions
                            .last_inbox_normal_contents_table_query_rows_handle
                            != released_handle =>
                    {
                        Some(format!(
                            "request_id={request_id};request_rops={request_rop_names};handle={};folder=0x{folder_id:016x};position={};row_count={};columns={};column_support={};normal_message_defaulted_column_detail={};sort={};restriction={};last_setcolumns={};last_query_rows={};view_handoff={};descriptor_behavior={}",
                            format_optional_debug_handle(released_handle),
                            position,
                            folder_message_count(*folder_id, mailboxes, emails, snapshot),
                            format_debug_property_tags(columns),
                            normal_message_table_column_support_summary(columns),
                            normal_message_defaulted_column_detail(columns),
                            format_debug_sort_orders(sort_orders),
                            format_debug_restriction_option(restriction.as_ref()),
                            debug_context_or_none(
                                &session
                                    .post_hierarchy_actions
                                    .last_inbox_normal_contents_table_setcolumns_context
                            ),
                            debug_context_or_none(
                                &session
                                    .post_hierarchy_actions
                                    .last_inbox_normal_contents_table_query_rows_context
                            ),
                            format_outlook_view_handoff_table_contract(
                                *folder_id,
                                *associated,
                                columns,
                                snapshot,
                            ),
                            format_inbox_view_descriptor_set_columns_behavior_contract(
                                *folder_id,
                                *associated,
                                columns,
                                snapshot,
                            )
                        ))
                    }
                    _ => None,
                };
                let calendar_normal_release_context = match released_object {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => Some(format!(
                        "request_id={request_id};request_rops={request_rop_names};handle={};position={};row_count={};columns={};sort={};restriction={};view_handoff={}",
                        format_optional_debug_handle(released_handle),
                        position,
                        folder_message_count(*folder_id, mailboxes, emails, snapshot),
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(sort_orders),
                        format_debug_restriction_option(restriction.as_ref()),
                        format_outlook_view_handoff_table_contract(
                            *folder_id,
                            *associated,
                            columns,
                            snapshot,
                        )
                    )),
                    _ => None,
                };
                let post_inbox_fai_handoff_context = match released_object {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID
                        && *associated
                        && session
                            .post_hierarchy_actions
                            .inbox_associated_contents_table_observed
                        && !session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_observed
                        && !session.post_hierarchy_actions.post_inbox_fai_handoff_logged =>
                    {
                        let filtered_row_count = restricted_associated_folder_message_count(
                            *folder_id,
                            snapshot,
                            restriction.as_ref(),
                            principal.account_id,
                        );
                        let unfiltered_row_count =
                            associated_folder_message_count(*folder_id, snapshot);
                        Some((
                            format!(
                                "handle={};folder=0x{folder_id:016x};position={position};columns={};sort={};restriction={};filtered_row_count={};unfiltered_row_count={};handoff_visibility={}",
                                format_optional_debug_handle(released_handle),
                                format_debug_property_tags(columns),
                                format_debug_sort_orders(sort_orders),
                                restriction
                                    .as_ref()
                                    .map(format_debug_parsed_restriction)
                                    .unwrap_or_default(),
                                filtered_row_count,
                                unfiltered_row_count,
                                format_inbox_fai_handoff_visibility_context(
                                    snapshot,
                                    restriction.as_ref(),
                                    principal.account_id,
                                )
                            ),
                            format_inbox_post_fai_handoff_context(
                                &session.post_hierarchy_actions,
                            ),
                            format_live_handle_debug_summary(session),
                        ))
                    }
                    _ => None,
                };
                let post_fai_hierarchy_release_without_inbox_contents =
                    format_post_fai_hierarchy_release_without_inbox_contents_context(
                        released_object,
                        released_handle,
                        &session.post_hierarchy_actions,
                        mailboxes,
                        snapshot,
                    );
                if session.hierarchy_sync_completed() {
                    let remaining_before = session.handles.len();
                    post_hierarchy_release_events.push(PostHierarchyReleaseDebugEvent {
                        input_handle_index: request.input_handle_index().unwrap_or(0),
                        handle: format_optional_debug_handle(released_handle),
                        object_kind: released_object_kind.to_string(),
                        folder_id: released_folder_id.clone(),
                        remaining_before,
                        remaining_after: remaining_before,
                        logon_before_content_sync: matches!(
                            released_object,
                            Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
                        ) && !session
                            .post_hierarchy_actions
                            .content_sync_configure_observed,
                    });
                }
                if matches!(
                    released_object,
                    Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
                ) {
                    session.record_logoff_after_hierarchy_completion();
                }
                if let Err(error) = persist_released_associated_config_stream(
                    store,
                    principal,
                    session,
                    released_object_for_stream_persist.as_ref(),
                )
                .await
                {
                    tracing::warn!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x01",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(released_handle),
                        error = %error,
                        "mapi associated config stream release persist failed"
                    );
                }
                release_handle_slot(session, &mut handle_slots, &request);
                if let Some(handle) = released_handle {
                    same_execute_released_handles.insert(handle);
                }
                if let Some(context) = inbox_related_release_context {
                    session.record_last_inbox_related_release_context(context);
                }
                session.record_last_table_release_context(format!(
                    "phase=release;request_id={request_id};request_rops={request_rop_names};input_index={};handle={};kind={};folder={};role={};associated={}",
                    request.input_handle_index().unwrap_or(0),
                    format_optional_debug_handle(released_handle),
                    released_object_kind,
                    released_folder_id,
                    released_folder_role,
                    released_associated_contents_table
                ));
                if let Some(context) = visible_inbox_release_without_query_rows {
                    let has_defaulted_columns =
                        context.contains(";defaulted=0x") || context.contains("backed=false");
                    if has_defaulted_columns {
                        session.record_outlook_view_failure_trace_event(format!(
                            "visible_inbox_release_without_query_rows:{context}"
                        ));
                        tracing::warn!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = %request_id,
                            request_rop_id = "0x01",
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            input_handle_value = %format_optional_debug_handle(released_handle),
                            release_without_query_rows_context = %context,
                            "rca debug mapi visible inbox released before query rows"
                        );
                    } else {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = %request_id,
                            request_rop_id = "0x01",
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            input_handle_value = %format_optional_debug_handle(released_handle),
                            release_without_query_rows_context = %context,
                            "rca debug mapi visible inbox released before query rows"
                        );
                    }
                }
                if let Some(context) = calendar_normal_release_context {
                    session.record_outlook_view_failure_trace_event(format!(
                        "calendar_normal_release:{context}"
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x01",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(released_handle),
                        release_context = %context,
                        "rca debug mapi calendar normal released"
                    );
                }
                if let Some((released_table_context, handoff_context, live_handle_summary)) =
                    post_inbox_fai_handoff_context
                {
                    record_mapi_outlook_view_inbox_fai_handoff_without_contents();
                    record_mapi_outlook_view_bootstrap_stall(1);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x01",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(released_handle),
                        released_table_context = %released_table_context,
                        handoff_context = %handoff_context,
                        live_handle_summaries_before_release = %live_handle_summary,
                        remaining_handle_count_after_release = session.handles.len(),
                        "rca debug mapi inbox associated handoff without contents"
                    );
                    session.mark_post_inbox_fai_handoff_logged();
                }
                if let Some(context) = post_fai_hierarchy_release_without_inbox_contents {
                    record_mapi_outlook_view_post_fai_hierarchy_without_contents();
                    record_mapi_outlook_view_bootstrap_stall(2);
                    session.record_outlook_view_failure_trace_event(format!(
                        "post_fai_hierarchy_release_without_inbox_contents:{context}"
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %request_id,
                        request_rop_id = "0x01",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(released_handle),
                        release_context = %context,
                        live_handle_summaries_after_release = %format_live_handle_debug_summary(session),
                        "rca debug mapi post fai hierarchy released without inbox contents"
                    );
                }
                if let Some(event) = post_hierarchy_release_events.last_mut() {
                    event.remaining_after = session.handles.len();
                }
                session.record_recent_probe_action(format!(
                    "Release(in={},handle={},kind={},folder={})",
                    request.input_handle_index().unwrap_or(0),
                    format_optional_debug_handle(released_handle),
                    released_object_kind,
                    released_folder_id
                ));
                tracing::debug!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x01",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = %format_optional_debug_handle(released_handle),
                    object_kind = released_object_kind,
                    folder_id = %released_folder_id,
                    inbox_related_release_context = %inbox_related_release_context_for_log,
                    remaining_handle_count = session.handles.len(),
                    "rca debug mapi release before inbox probe"
                );
            }
            Some(RopId::OpenFolder) => {
                let input_handle_value = input_handle(&handle_slots, &request);
                let input_object_kind =
                    mapi_object_debug_kind(input_object(session, &handle_slots, &request));
                let input_folder_id =
                    mapi_object_debug_folder_id(input_object(session, &handle_slots, &request));
                let input_context =
                    format_handle_lineage_context(input_object(session, &handle_slots, &request));
                let requested_folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
                let folder_id = session.resolve_special_folder_alias(requested_folder_id);
                let mailbox_folder = folder_row_for_id(folder_id, mailboxes);
                let mailbox_folder_found = mailbox_folder.is_some();
                let collaboration_folder_found =
                    snapshot.collaboration_folder_for_id(folder_id).is_some();
                let public_folder_found = snapshot.public_folder_for_id(folder_id).is_some();
                let search_folder_definition_found = !session
                    .search_folder_definition_was_deleted(folder_id)
                    && (snapshot
                        .search_folder_definition_for_folder_id(folder_id)
                        .is_some()
                        || session.search_folder_definition(folder_id).is_some());
                let advertised_special_folder = is_advertised_special_folder(folder_id);
                let (folder_name, folder_role, folder_container_class) =
                    debug_open_folder_metadata(folder_id, mailboxes);
                let open_folder_result = if mailbox_folder_found
                    || collaboration_folder_found
                    || public_folder_found
                    || search_folder_definition_found
                    || advertised_special_folder
                {
                    "success"
                } else {
                    "not_found"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x02",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.output_handle_index.unwrap_or(0),
                    open_mode_flags =
                        format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                    requested_folder_id = format!("0x{requested_folder_id:016x}"),
                    folder_id = format!("0x{folder_id:016x}"),
                    folder_alias_resolved = requested_folder_id != folder_id,
                    folder_name,
                    role = folder_role,
                    container_class = folder_container_class,
                    mailbox_folder_found = mailbox_folder_found,
                    collaboration_folder_found = collaboration_folder_found,
                    public_folder_found = public_folder_found,
                    search_folder_definition_found = search_folder_definition_found,
                    advertised_special_folder = advertised_special_folder,
                    result = open_folder_result,
                    message = "rca debug mapi open folder"
                );
                tracing::debug!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x02",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = %format_optional_debug_handle(input_handle_value),
                    input_object_kind,
                    input_folder_id,
                    input_handle_context = %input_context,
                    requested_folder_id = format!("0x{requested_folder_id:016x}"),
                    resolved_folder_id = format!("0x{folder_id:016x}"),
                    output_handle_index = request.output_handle_index.unwrap_or(0),
                    "rca debug mapi open folder handle lineage"
                );
                log_calendar_folder_contract(
                    principal,
                    folder_id,
                    mailbox_folder_found,
                    collaboration_folder_found,
                    advertised_special_folder,
                    snapshot,
                    mailboxes,
                    emails,
                );
                log_special_folder_contract(
                    principal,
                    request_id,
                    folder_id,
                    mailbox_folder_found,
                    collaboration_folder_found,
                    advertised_special_folder,
                    snapshot,
                    mailboxes,
                    emails,
                );
                if open_folder_result == "not_found" {
                    responses.extend_from_slice(&rop_error_response(
                        0x02,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                let is_public_folder_ghosted = public_folder_found
                    && snapshot
                        .public_folder_replica_server_names(folder_id)
                        .is_empty();
                session.record_opened_folder(folder_id);
                let properties = folder_properties_for_open(
                    store, principal, session, folder_id, mailboxes, snapshot,
                )
                .await;
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x02",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.output_handle_index.unwrap_or(0),
                    open_mode_flags =
                        format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                    folder_id = format!("0x{folder_id:016x}"),
                    folder_name = post_hierarchy_probe_folder_name(folder_id),
                    role = debug_role_for_folder_id(folder_id),
                    property_count = properties.len(),
                    property_shapes = %debug_open_folder_property_shapes(&properties),
                    message = "rca debug mapi open folder properties"
                );
                let inbox_contract_display_name =
                    mapi_value_debug_string(&properties, PID_TAG_DISPLAY_NAME_W);
                let inbox_contract_folder_type =
                    mapi_value_debug_u32(&properties, PID_TAG_FOLDER_TYPE);
                let inbox_contract_container_class =
                    mapi_value_debug_string(&properties, PID_TAG_CONTAINER_CLASS_W);
                let inbox_contract_record_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_RECORD_KEY);
                let inbox_contract_source_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_SOURCE_KEY);
                let inbox_contract_parent_source_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_PARENT_SOURCE_KEY);
                let inbox_contract_content_count =
                    mapi_value_debug_u32(&properties, PID_TAG_CONTENT_COUNT);
                let inbox_contract_unread_count =
                    mapi_value_debug_u32(&properties, PID_TAG_CONTENT_UNREAD_COUNT);
                let inbox_contract_subfolders =
                    mapi_value_debug_bool(&properties, PID_TAG_SUBFOLDERS);
                let root_ipm_contract_display_name =
                    mapi_value_debug_string(&properties, PID_TAG_DISPLAY_NAME_W);
                let root_ipm_contract_folder_type =
                    mapi_value_debug_u32(&properties, PID_TAG_FOLDER_TYPE);
                let root_ipm_contract_container_class =
                    mapi_value_debug_string(&properties, PID_TAG_CONTAINER_CLASS_W);
                let root_ipm_contract_record_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_RECORD_KEY);
                let root_ipm_contract_source_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_SOURCE_KEY);
                let root_ipm_contract_parent_source_key =
                    mapi_value_debug_binary_decode(&properties, PID_TAG_PARENT_SOURCE_KEY);
                let handle = session.allocate_output_handle_avoiding(
                    request.output_handle_index,
                    MapiObject::Folder {
                        folder_id,
                        properties,
                    },
                    &same_execute_released_handles,
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                let open_folder_response =
                    rop_open_folder_response(&request, is_public_folder_ghosted);
                if folder_id == INBOX_FOLDER_ID {
                    let first_loop_transition = format!(
                        "trigger=open_folder;open_probe_before={};folder_type_probe_before={};input_index={};input_handle={};input_kind={};input_folder={};input_context={};output_index={};output_handle={};open_mode=0x{:02x};requested_folder=0x{requested_folder_id:016x};resolved_folder=0x{folder_id:016x};alias_resolved={};recent_before={}",
                        session
                            .post_hierarchy_actions
                            .inbox_open_folder_probe_count,
                        session
                            .post_hierarchy_actions
                            .inbox_folder_type_getprops_probe_count,
                        request.input_handle_index().unwrap_or(0),
                        format_optional_debug_handle(input_handle_value),
                        input_object_kind,
                        input_folder_id,
                        input_context,
                        request.output_handle_index.unwrap_or(0),
                        handle,
                        request.payload.get(8).copied().unwrap_or(0),
                        requested_folder_id != folder_id,
                        session.post_hierarchy_actions.recent_probe_actions.join(">")
                    );
                    if session.post_hierarchy_actions.inbox_open_folder_probe_count >= 1
                        && !session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_observed
                    {
                        session.record_first_inbox_loop_transition_context(
                            first_loop_transition.clone(),
                        );
                    }
                    session.record_inbox_open_folder_probe();
                    session.record_last_inbox_open_folder_context(format!(
                        "input_index={};input_handle={};input_kind={};input_folder={};output_index={};output_handle={};open_mode=0x{:02x};display_name={};folder_type={};container_class={};content_count={};unread_count={};subfolders={};record_key={};source_key={};parent_source_key={};open_folder_response_bytes={};open_folder_response_preview={}",
                        request.input_handle_index().unwrap_or(0),
                        format_optional_debug_handle(input_handle_value),
                        input_object_kind,
                        input_folder_id,
                        request.output_handle_index.unwrap_or(0),
                        handle,
                        request.payload.get(8).copied().unwrap_or(0),
                        inbox_contract_display_name,
                        inbox_contract_folder_type,
                        inbox_contract_container_class,
                        inbox_contract_content_count,
                        inbox_contract_unread_count,
                        inbox_contract_subfolders,
                        inbox_contract_record_key,
                        inbox_contract_source_key,
                        inbox_contract_parent_source_key,
                        open_folder_response.len(),
                        hex_preview(&open_folder_response, 32)
                    ));
                    session.record_recent_probe_action(format!(
                        "OpenFolder(in={},handle={},out={},folder=0x{folder_id:016x})",
                        request.input_handle_index().unwrap_or(0),
                        format_optional_debug_handle(input_handle_value),
                        handle
                    ));
                }
                let post_fai_reopen_stall = folder_id == INBOX_FOLDER_ID
                    && inbox_post_fai_reopen_stall_observed(&session.post_hierarchy_actions)
                    && !session.post_hierarchy_actions.post_inbox_fai_reopen_logged;
                responses.extend_from_slice(&open_folder_response);
                session.record_post_hierarchy_request_contract(
                    post_hierarchy_open_folder_contract(folder_id, "ok"),
                );
                if folder_id == INBOX_FOLDER_ID {
                    if post_fai_reopen_stall {
                        tracing::warn!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x02",
                            mapi_request_id = request_id,
                            folder_id = format!("0x{folder_id:016x}"),
                            output_handle_id = handle,
                            open_mode_flags =
                                format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                            open_folder_response_bytes = open_folder_response.len(),
                            open_folder_response_preview = %hex_preview(&open_folder_response, 32),
                            last_open = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_open_folder_context
                            ),
                            last_associated_query = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_associated_query_context
                            ),
                            last_associated_find = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_associated_find_context
                            ),
                            last_inbox_related_release = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_related_release_context
                            ),
                            last_folder_type_getprops = %debug_context_or_none(
                                &session
                                    .post_hierarchy_actions
                                    .last_inbox_folder_type_getprops_context
                            ),
                            recent_actions =
                                %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                            expected_next_client_step =
                                "Open Inbox normal contents table or SynchronizationConfigure",
                            "rca warn mapi inbox reopened after associated FAI without normal contents"
                        );
                        session.mark_post_inbox_fai_reopen_logged();
                    }
                    if session
                        .post_hierarchy_actions
                        .inbox_rule_organizer_stream_read_observed
                        && !session
                            .post_hierarchy_actions
                            .post_rule_organizer_stream_reopen_logged
                    {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x02",
                            mapi_request_id = request_id,
                            folder_id = format!("0x{folder_id:016x}"),
                            output_handle_id = handle,
                            open_mode_flags =
                                format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                            open_folder_response_bytes = open_folder_response.len(),
                            open_folder_response_preview = %hex_preview(&open_folder_response, 32),
                            last_rule_organizer_stream = %debug_context_or_none(
                                &session
                                    .post_hierarchy_actions
                                    .last_inbox_rule_organizer_stream_context
                            ),
                            last_open = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_open_folder_context
                            ),
                            last_contents_table = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_contents_table_context
                            ),
                            last_associated_query = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_associated_query_context
                            ),
                            last_associated_find = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_associated_find_context
                            ),
                            last_inbox_related_release = %debug_context_or_none(
                                &session.post_hierarchy_actions.last_inbox_related_release_context
                            ),
                            last_folder_type_getprops = %debug_context_or_none(
                                &session
                                    .post_hierarchy_actions
                                    .last_inbox_folder_type_getprops_context
                            ),
                            recent_actions =
                                %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                            "rca debug mapi inbox reopened after RuleOrganizer stream read"
                        );
                        session.mark_post_rule_organizer_stream_reopen_logged();
                    }
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x02",
                        folder_id = format!("0x{folder_id:016x}"),
                        output_handle_id = handle,
                        display_name = %inbox_contract_display_name,
                        folder_type = %inbox_contract_folder_type,
                        container_class = %inbox_contract_container_class,
                        record_key = %inbox_contract_record_key,
                        source_key = %inbox_contract_source_key,
                        parent_source_key = %inbox_contract_parent_source_key,
                        content_count = %inbox_contract_content_count,
                        unread_count = %inbox_contract_unread_count,
                        subfolders = %inbox_contract_subfolders,
                        "rca debug mapi opened inbox folder handle contract"
                    );
                    if let Some(summary) =
                        format_inbox_open_loop_summary(&session.post_hierarchy_actions)
                    {
                        if !session.post_hierarchy_actions.inbox_loop_transition_logged {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x02",
                                folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                                transition_context =
                                    %debug_context_or_none(
                                        &session
                                            .post_hierarchy_actions
                                            .first_inbox_loop_transition_context
                                    ),
                                loop_summary = %summary,
                                "rca debug mapi inbox open loop transition"
                            );
                            session.mark_inbox_loop_transition_logged();
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                            loop_summary = %summary,
                            "rca debug mapi repeated inbox open folder loop summary"
                        );
                    }
                }
                if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x02",
                        folder_id = format!("0x{folder_id:016x}"),
                        folder_name = post_hierarchy_probe_folder_name(folder_id),
                        output_handle_id = handle,
                        display_name = %root_ipm_contract_display_name,
                        folder_type = %root_ipm_contract_folder_type,
                        container_class = %root_ipm_contract_container_class,
                        record_key = %root_ipm_contract_record_key,
                        source_key = %root_ipm_contract_source_key,
                        parent_source_key = %root_ipm_contract_parent_source_key,
                        default_folder_identification_contract =
                            %default_folder_identification_contract_for_debug(principal),
                        "rca debug mapi root ipm subtree folder handle contract"
                    );
                    log_outlook_bootstrap_phase(
                        principal,
                        "root_ipm_subtree_opened",
                        "0x02",
                        Some(folder_id),
                        false,
                        None,
                        None,
                        Some(handle),
                        "",
                    );
                } else if folder_id == INBOX_FOLDER_ID {
                    log_outlook_bootstrap_phase(
                        principal,
                        "inbox_opened",
                        "0x02",
                        Some(folder_id),
                        false,
                        None,
                        None,
                        Some(handle),
                        "",
                    );
                }
                output_handles.push(handle);
            }
            Some(RopId::OpenMessage) => {
                let message_id = request.message_id().unwrap_or(0);
                let folder_id = open_message_folder_id(&request, message_id);
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                            saved_email: None,
                            pending_properties: HashMap::new(),
                        },
                    );
                    session.record_message_handle_generation(handle, folder_id, message_id);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    let response = rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    );
                    log_open_message_debug(
                        principal,
                        &request,
                        handle,
                        folder_id,
                        message_id,
                        "mailbox",
                        email,
                        response.len(),
                    );
                    responses.extend_from_slice(&response);
                    output_handles.push(handle);
                } else if let Some(message) =
                    search_folder_message_for_id(snapshot, folder_id, message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                            saved_email: None,
                            pending_properties: HashMap::new(),
                        },
                    );
                    session.record_message_handle_generation(handle, folder_id, message_id);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    let response = rop_open_message_response(
                        &request,
                        &message.email.subject,
                        message_recipients(&message.email).len(),
                    );
                    log_open_message_debug(
                        principal,
                        &request,
                        handle,
                        folder_id,
                        message_id,
                        "search_folder",
                        &message.email,
                        response.len(),
                    );
                    responses.extend_from_slice(&response);
                    output_handles.push(handle);
                } else if let Some(email) = unique_message_for_id(message_id, emails) {
                    let canonical_folder_id = canonical_message_folder_id(email, mailboxes);
                    let handle_folder_id =
                        fallback_open_message_folder_id(folder_id, email, mailboxes);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x03",
                        requested_folder_id = %format!("0x{folder_id:016x}"),
                        canonical_folder_id = %format!("0x{canonical_folder_id:016x}"),
                        handle_folder_id = %format!("0x{handle_folder_id:016x}"),
                        message_id = %format!("0x{message_id:016x}"),
                        message_subject = %email.subject,
                        fallback_reason = "unique_message_id_folder_mismatch",
                        "rca debug mapi open message folder fallback"
                    );
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id: handle_folder_id,
                            message_id,
                            saved_email: None,
                            pending_properties: HashMap::new(),
                        },
                    );
                    session.record_message_handle_generation(handle, handle_folder_id, message_id);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    let response = rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    );
                    log_open_message_debug(
                        principal,
                        &request,
                        handle,
                        handle_folder_id,
                        message_id,
                        "unique_message_id_folder_mismatch",
                        email,
                        response.len(),
                    );
                    responses.extend_from_slice(&response);
                    output_handles.push(handle);
                } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Contact {
                            folder_id,
                            contact_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &contact.contact.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &event.event.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Task {
                            folder_id,
                            task_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &task.task.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &note.note.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &entry.entry.subject,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(message) =
                    common_view_named_view_message_for_open(snapshot, folder_id, message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::CommonViewNamedView {
                            folder_id,
                            view_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    log_outlook_view_handoff(
                        principal, &request, folder_id, message_id, handle, &message, snapshot,
                    );
                    session.record_outlook_view_failure_trace_event(format!(
                        "view_handoff:request_id={request_id};folder=0x{folder_id:016x};view=0x{message_id:016x};handle={handle};class={};name={}",
                        "IPM.Microsoft.FolderDesign.NamedView",
                        message.name
                    ));
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &message.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(definition) =
                    search_folder_definition_message_for_open(snapshot, folder_id, message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::SearchFolderDefinitionMessage {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &definition.display_name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if folder_id == COMMON_VIEWS_FOLDER_ID {
                    if let Some(message) =
                        navigation_shortcut_message_for_open(snapshot, folder_id, message_id)
                    {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::NavigationShortcut {
                                folder_id,
                                shortcut_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &message.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
                    if let Some(message) =
                        delegate_freebusy_message_for_open(snapshot, folder_id, message_id)
                    {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::DelegateFreeBusyMessage {
                                folder_id,
                                message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &message.message.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    if let Some(message) =
                        conversation_action_message_for_open(snapshot, folder_id, message_id)
                    {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::ConversationAction {
                                folder_id,
                                conversation_action_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &conversation_action_subject(&message.action),
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if let Some(message) = snapshot
                    .associated_config_message_for_id(message_id)
                    .filter(|message| message.folder_id == folder_id)
                    .or_else(|| {
                        snapshot
                            .associated_config_message_for_identity_id(message_id)
                            .filter(|message| message.folder_id == folder_id)
                            .inspect(|message| {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = format_args!("0x{:02x}", request.rop_id),
                                    folder_id = format_args!("0x{folder_id:016x}"),
                                    requested_message_id = format_args!("0x{message_id:016x}"),
                                    canonical_config_id = %message.canonical_id,
                                    modeled_config_id = format_args!("0x{:016x}", message.id),
                                    message_class = %message.message_class,
                                    "rca debug mapi opened virtual associated config identity"
                                );
                            })
                    })
                    .or_else(|| {
                        snapshot.associated_config_message_for_folder_and_source_key_id(
                            folder_id, message_id,
                        )
                    })
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::AssociatedConfig {
                            folder_id,
                            config_id: message.id,
                            saved_message: Some(message.clone()),
                        },
                    );
                    if folder_id == INBOX_FOLDER_ID
                        && (message.message_class.starts_with("IPM.Configuration.")
                            || message.message_class == "IPM.ExtendedRule.Message")
                    {
                        session.record_inbox_associated_config_open();
                        session.record_outlook_view_failure_trace_event(format!(
                            "open_inbox_config:request_id={request_id};folder=0x{folder_id:016x};config=0x{:016x};handle={handle};class={};subject={}",
                            message.id,
                            message.message_class,
                            message.subject
                        ));
                        session.record_recent_probe_action(format!(
                            "OpenAssociatedConfig(out={},folder=0x{folder_id:016x},id=0x{:016x},class={})",
                            request.output_handle_index.unwrap_or(0),
                            message.id,
                            message.message_class
                        ));
                    }
                    let response = rop_open_message_response(&request, &message.subject, 0);
                    if is_contact_link_timestamp_config(folder_id, &message.message_class) {
                        session.record_outlook_view_failure_trace_event(format!(
                            "open_contact_link_timestamp_config:request_id={request_id};folder=0x{folder_id:016x};config=0x{:016x};handle={handle};subject={}",
                            message.id,
                            message.subject
                        ));
                    }
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x03",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        output_handle_index = request.output_handle_index.unwrap_or(0),
                        output_handle = handle,
                        folder_id = format_args!("0x{folder_id:016x}"),
                        associated_config_id = format_args!("0x{:016x}", message.id),
                        associated_config_canonical_id = %message.canonical_id,
                        associated_config_class = %message.message_class,
                        associated_config_subject = %message.subject,
                        open_message_payload_preview = %hex_preview(&request.payload, 48),
                        open_message_response_bytes = response.len(),
                        open_message_response_preview = %hex_preview(&response, 96),
                        associated_config_shape = %associated_config_open_shape(&message),
                        contacts_surface = mapi_folder_is_outlook_contacts_surface(folder_id),
                        "rca debug mapi open associated config"
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&response);
                    output_handles.push(handle);
                } else if snapshot.associated_config_identity_matches_folder(folder_id, message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::PendingAssociatedMessage {
                            folder_id,
                            properties: HashMap::new(),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        "IPM.Configuration",
                        0,
                    ));
                    output_handles.push(handle);
                } else if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
                    if let Some(item) = snapshot.recoverable_item_for_id(folder_id, message_id) {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::RecoverableItem {
                                folder_id,
                                item_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &item.item.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if snapshot.public_folder_for_id(folder_id).is_some() {
                    if let Some(item) = snapshot.public_folder_item_for_id(folder_id, message_id) {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::PublicFolderItem {
                                folder_id,
                                item_id: message_id,
                                properties: HashMap::new(),
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &item.item.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x03",
                        requested_folder_id = %format!("0x{folder_id:016x}"),
                        requested_folder_role = debug_role_for_folder_id(folder_id),
                        message_id = %format!("0x{message_id:016x}"),
                        loaded_email_count = emails.len(),
                        same_id_email_count = emails
                            .iter()
                            .filter(|email| mapi_item_id_matches(&email.id, message_id))
                            .count(),
                        failure_reason = "open_message_not_found",
                        "rca debug mapi open message failure"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x03,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::GetHierarchyTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                if !hierarchy_table_flags_are_valid(&request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x04,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(ROOT_FOLDER_ID);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    hierarchy_table_object(
                        folder_id,
                        session.deleted_advertised_special_folders.clone(),
                    ),
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                let row_count = if folder_id == PUBLIC_FOLDERS_ROOT_FOLDER_ID
                    && snapshot.public_folders().is_empty()
                {
                    store
                        .fetch_public_folder_trees(principal.account_id)
                        .await
                        .map(|trees| {
                            trees
                                .iter()
                                .filter(|tree| tree.root_folder_id.is_some())
                                .count()
                                .min(u32::MAX as usize) as u32
                        })
                        .unwrap_or(0)
                } else {
                    hierarchy_row_count_excluding_deleted(
                        folder_id,
                        mailboxes,
                        snapshot,
                        &session.deleted_advertised_special_folders,
                    )
                };
                responses.extend_from_slice(&get_hierarchy_table_response(&request, row_count));
                if folder_id == INBOX_FOLDER_ID {
                    session.record_last_inbox_hierarchy_table_context(format!(
                        "input_index={};output_index={};output_handle={};row_count={row_count};expected_subfolders=false",
                        request.input_handle_index().unwrap_or(0),
                        request.output_handle_index.unwrap_or(0),
                        handle
                    ));
                    session.record_recent_probe_action(format!(
                        "GetHierarchyTable(in={},out={},row_count={row_count})",
                        request.input_handle_index().unwrap_or(0),
                        handle
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x04",
                        folder_id = %format!("0x{folder_id:016x}"),
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        output_handle_index = request.output_handle_index.unwrap_or(0),
                        output_handle_value = handle,
                        hierarchy_row_count = row_count,
                        expected_subfolders = false,
                        normal_contents_table_observed =
                            session.post_hierarchy_actions.inbox_normal_contents_table_observed,
                        associated_contents_table_observed =
                            session.post_hierarchy_actions.inbox_associated_contents_table_observed,
                        message = "rca debug mapi inbox hierarchy table opened"
                    );
                }
                if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) {
                    log_outlook_bootstrap_phase(
                        principal,
                        "hierarchy_table_opened",
                        "0x04",
                        Some(folder_id),
                        false,
                        Some(row_count),
                        None,
                        Some(handle),
                        "",
                    );
                }
                output_handles.push(handle);
            }
            Some(RopId::GetContentsTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                let Some(input_object) = input_object(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                let folder_id = match input_object {
                    MapiObject::Folder { folder_id, .. } => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x05,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let table_flags = request.payload.first().copied().unwrap_or(0);
                if let Some(error) = contents_table_flags_error(
                    table_flags,
                    folder_id,
                    snapshot.public_folder_for_id(folder_id).is_some(),
                ) {
                    responses.extend_from_slice(&rop_error_response(
                        0x05,
                        request.output_handle_index.unwrap_or(0),
                        error,
                    ));
                    continue;
                }
                let associated = table_flags & 0x02 != 0;
                let contents_folder_id = if table_flags & 0x80 != 0
                    && folder_id == ROOT_FOLDER_ID
                    && snapshot.public_folder_for_id(folder_id).is_none()
                {
                    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID
                } else {
                    folder_id
                };
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_read)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x05,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    contents_table_object(contents_folder_id, associated),
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                let row_count = if associated {
                    associated_folder_message_count(contents_folder_id, snapshot)
                } else {
                    folder_message_count(contents_folder_id, mailboxes, emails, snapshot)
                };
                log_outlook_contents_table_open(
                    principal,
                    request_id,
                    &request,
                    contents_folder_id,
                    table_flags,
                    associated,
                    row_count,
                    handle,
                    snapshot,
                );
                session.record_last_table_context(format!(
                    "phase=open;request_id={request_id};request_rops={request_rop_names};input_index={};output_index={};handle={};folder=0x{contents_folder_id:016x};role={};associated={associated};table_flags=0x{table_flags:02x};row_count={row_count}",
                    request.input_handle_index().unwrap_or(0),
                    request.output_handle_index.unwrap_or(0),
                    handle,
                    debug_role_for_folder_id(contents_folder_id)
                ));
                if contents_folder_id == CALENDAR_FOLDER_ID && !associated {
                    session.record_outlook_view_failure_trace_event(format!(
                        "calendar_normal_table_open:request_id={request_id};handle={handle};row_count={row_count};flags=0x{table_flags:02x}"
                    ));
                }
                if folder_id == INBOX_FOLDER_ID {
                    session.record_outlook_view_failure_trace_event(format!(
                        "inbox_contents_table_open:request_id={request_id};handle={handle};associated={associated};row_count={row_count};flags=0x{table_flags:02x}"
                    ));
                    if associated {
                        session.record_inbox_associated_contents_table();
                    } else {
                        session.record_inbox_normal_contents_table();
                        record_mapi_outlook_view_inbox_normal_contents_opened();
                    }
                    session.record_last_inbox_contents_table_context(format!(
                        "input_index={};output_index={};output_handle={};table_flags=0x{table_flags:02x};associated={associated};row_count={row_count}",
                        request.input_handle_index().unwrap_or(0),
                        request.output_handle_index.unwrap_or(0),
                        handle
                    ));
                    if !associated {
                        session.record_recent_probe_action(format!(
                            "GetContentsTable(in={},out={},associated=false,row_count={row_count})",
                            request.input_handle_index().unwrap_or(0),
                            handle
                        ));
                    }
                }
                if associated && folder_id == COMMON_VIEWS_FOLDER_ID {
                    log_outlook_bootstrap_phase(
                        principal,
                        "common_views_associated_table_opened",
                        "0x05",
                        Some(folder_id),
                        associated,
                        Some(row_count),
                        None,
                        Some(handle),
                        "",
                    );
                } else if associated && folder_id == INBOX_FOLDER_ID {
                    log_outlook_bootstrap_phase(
                        principal,
                        "inbox_associated_table_opened",
                        "0x05",
                        Some(folder_id),
                        associated,
                        Some(row_count),
                        None,
                        Some(handle),
                        "",
                    );
                } else if !associated && folder_id == INBOX_FOLDER_ID {
                    log_outlook_bootstrap_phase(
                        principal,
                        "inbox_contents_table_opened",
                        "0x05",
                        Some(folder_id),
                        associated,
                        Some(row_count),
                        None,
                        Some(handle),
                        "",
                    );
                }
                responses.extend_from_slice(&get_contents_table_response(&request, row_count));
                output_handles.push(handle);
            }
            Some(RopId::CreateMessage) => {
                let folder_id = request.folder_id().unwrap_or_else(|| {
                    input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                });
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }
                if snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && folder_row_for_id(folder_id, mailboxes).is_none()
                    && snapshot.public_folder_for_id(folder_id).is_none()
                    && folder_id != CALENDAR_FOLDER_ID
                    && !synthetic_folder_allows_create_message(folder_id)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let created_at = current_mapi_filetime();
                let initial_message_properties = || {
                    HashMap::from([
                        (PID_TAG_CREATION_TIME, MapiValue::U64(created_at)),
                        (PID_TAG_LAST_MODIFICATION_TIME, MapiValue::U64(created_at)),
                    ])
                };
                let pending_object = if request.create_message_associated()
                    && !matches!(
                        folder_id,
                        COMMON_VIEWS_FOLDER_ID | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
                    ) {
                    MapiObject::PendingAssociatedMessage {
                        folder_id,
                        properties: initial_message_properties(),
                    }
                } else {
                    match snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| folder.kind)
                    {
                        Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        None if folder_id == CALENDAR_FOLDER_ID => MapiObject::PendingEvent {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        Some(MapiCollaborationFolderKind::Task) => MapiObject::PendingTask {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        _ if folder_id == NOTES_FOLDER_ID => MapiObject::PendingNote {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        _ if folder_id == JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                            folder_id,
                            properties: initial_message_properties(),
                        },
                        _ if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID => {
                            MapiObject::PendingConversationAction {
                                folder_id,
                                properties: initial_message_properties(),
                            }
                        }
                        _ if folder_id == FREEBUSY_DATA_FOLDER_ID => {
                            MapiObject::PendingAssociatedMessage {
                                folder_id,
                                properties: initial_message_properties(),
                            }
                        }
                        _ if folder_id == COMMON_VIEWS_FOLDER_ID => {
                            MapiObject::PendingNavigationShortcut {
                                folder_id,
                                properties: initial_message_properties(),
                            }
                        }
                        _ => MapiObject::PendingMessage {
                            folder_id,
                            properties: initial_message_properties(),
                            recipients: Vec::new(),
                        },
                    }
                };
                let handle =
                    session.allocate_output_handle(request.output_handle_index, pending_object);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_message_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPropertiesSpecific) => {
                echo_input_handle_table = true;
                let is_inbox_folder_type_probe = matches!(
                    input_object(session, &handle_slots, &request),
                    Some(MapiObject::Folder {
                        folder_id: INBOX_FOLDER_ID,
                        ..
                    })
                ) && request
                    .property_tags()
                    .iter()
                    .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_FOLDER_TYPE);
                if is_inbox_folder_type_probe {
                    let input_handle_value = input_handle(&handle_slots, &request);
                    session.record_inbox_folder_type_getprops_probe();
                    session.record_recent_probe_action(format!(
                        "GetPropertiesSpecific(in={},handle={},tags={})",
                        request.input_handle_index().unwrap_or(0),
                        format_optional_debug_handle(input_handle_value),
                        format_debug_property_tags(&request.property_tags())
                    ));
                    if let Some(summary) =
                        format_inbox_open_loop_summary(&session.post_hierarchy_actions)
                    {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x07",
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            input_handle_value = %format_optional_debug_handle(input_handle_value),
                            folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                            loop_summary = %summary,
                            "rca debug mapi repeated inbox open folder loop summary"
                        );
                    }
                }
                let object = input_object(session, &handle_slots, &request);
                let visible_emails;
                let emails_for_request = if created_emails.is_empty() {
                    emails
                } else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x07",
                        object_kind = mapi_object_debug_kind(object),
                        folder_id = %mapi_object_debug_folder_id(object),
                        same_execute_created_email_count = created_emails.len(),
                        base_snapshot_email_count = emails.len(),
                        "rca debug mapi same execute created message visibility"
                    );
                    visible_emails = emails
                        .iter()
                        .chain(created_emails.iter())
                        .cloned()
                        .collect::<Vec<_>>();
                    &visible_emails
                };
                let custom_values = fetch_custom_property_values_for_request(
                    store,
                    principal,
                    object,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    &request.property_tags(),
                )
                .await
                .unwrap_or_default();
                let inbox_folder_type_getprops_context = if let (
                    true,
                    Some(MapiObject::Folder { properties, .. }),
                ) = (is_inbox_folder_type_probe, object)
                {
                    Some(format!(
                            "input_index={};input_handle={};requested_tags={};folder_type={};display_name={};container_class={};content_count={};unread_count={};associated_count={}",
                            request.input_handle_index().unwrap_or(0),
                            format_optional_debug_handle(input_handle(&handle_slots, &request)),
                            format_debug_property_tags(&request.property_tags()),
                            mapi_value_debug_u32(properties, PID_TAG_FOLDER_TYPE),
                            mapi_value_debug_string(properties, PID_TAG_DISPLAY_NAME_W),
                            mapi_value_debug_string(properties, PID_TAG_CONTAINER_CLASS_W),
                            mapi_value_debug_u32(properties, PID_TAG_CONTENT_COUNT),
                            mapi_value_debug_u32(properties, PID_TAG_CONTENT_UNREAD_COUNT),
                            mapi_value_debug_u32(properties, PID_TAG_ASSOCIATED_CONTENT_COUNT)
                        ))
                } else {
                    None
                };
                let named_property_context =
                    format_debug_named_property_context(session, &request.property_tags());
                let inbox_config_getprops_trace = if let Some(MapiObject::AssociatedConfig {
                    folder_id: INBOX_FOLDER_ID,
                    config_id,
                    saved_message,
                }) = object
                {
                    let (message_class, subject) = saved_message
                        .as_ref()
                        .map(|message| (message.message_class.as_str(), message.subject.as_str()))
                        .unwrap_or(("missing_saved_message", ""));
                    Some(format!(
                        "getprops_inbox_config:request_id={request_id};handle={};config=0x{config_id:016x};class={message_class};subject={subject};tags={};named_properties={}",
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        format_debug_property_tags(&request.property_tags()),
                        named_property_context
                    ))
                } else {
                    None
                };
                if !named_property_context.is_empty() {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x07",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = mapi_object_debug_kind(object),
                        folder_id = %mapi_object_debug_folder_id(object),
                        requested_property_tags = %format_debug_property_tags(&request.property_tags()),
                        named_property_context = %named_property_context,
                        "rca debug mapi get properties named property context"
                    );
                }
                let property_response = rop_get_properties_specific_response_with_custom(
                    &request,
                    object,
                    principal,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    &custom_values,
                );
                log_message_getprops_response_debug(
                    principal,
                    &request,
                    object,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    property_response.len(),
                );
                log_get_properties_specific_response_debug(
                    principal,
                    request_id,
                    &request,
                    object,
                    &property_response,
                );
                log_get_properties_view_response_debug(
                    principal,
                    request_id,
                    &request,
                    object,
                    &property_response,
                );
                log_get_properties_default_folder_response_debug(
                    principal,
                    request_id,
                    &request,
                    object,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    &property_response,
                );
                let post_hierarchy_contract =
                    post_hierarchy_getprops_contract(&request, object, &property_response);
                if should_log_outlook_surface_getprops_info(object) {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = request_id,
                        request_rop_id = "0x07",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = mapi_object_debug_kind(object),
                        folder_id = %mapi_object_debug_folder_id(object),
                        getprops_contract = %post_hierarchy_contract,
                        "rca debug mapi outlook surface getprops contract"
                    );
                }
                session.record_post_hierarchy_getprops_contract(post_hierarchy_contract.clone());
                session.record_post_hierarchy_request_contract(format!(
                    "{post_hierarchy_contract}->ok"
                ));
                responses.extend_from_slice(&property_response);
                if is_inbox_folder_type_probe {
                    if let Some(context) = inbox_folder_type_getprops_context {
                        session.record_last_inbox_folder_type_getprops_context(format!(
                            "{};{}",
                            context,
                            format_inbox_folder_type_getprops_response_context(&property_response)
                        ));
                    }
                    if let Some(context) = format_post_fai_folder_type_probe_loop_context(
                        &session.post_hierarchy_actions,
                    ) {
                        record_mapi_outlook_view_bootstrap_stall(3);
                        session.record_outlook_view_failure_trace_event(format!(
                            "post_fai_folder_type_probe_loop:{context}"
                        ));
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x07",
                            mapi_request_id = request_id,
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            input_handle_value =
                                %format_optional_debug_handle(input_handle(&handle_slots, &request)),
                            folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                            probe_loop_context = %context,
                            "rca debug mapi post fai inbox folder type probe loop"
                        );
                        session.mark_post_inbox_fai_folder_type_probe_loop_logged();
                    }
                    if let Some(summary) =
                        format_inbox_open_loop_summary(&session.post_hierarchy_actions)
                    {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x07",
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            input_handle_value =
                                %format_optional_debug_handle(input_handle(&handle_slots, &request)),
                            folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                            loop_summary = %summary,
                            "rca debug mapi repeated inbox open folder loop summary"
                        );
                    }
                }
                if let Some(trace) = inbox_config_getprops_trace {
                    session.record_outlook_view_failure_trace_event(trace);
                }
            }
            Some(RopId::GetPropertiesAll) => {
                responses.extend_from_slice(&rop_get_properties_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::GetPropertiesList) => {
                responses.extend_from_slice(&rop_get_properties_list_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ))
            }
            Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate) => {
                echo_input_handle_table = true;
                let set_properties_object = input_object(session, &handle_slots, &request).cloned();
                let set_properties_probe = set_properties_probe_request(&request);
                log_set_properties_specific_debug(
                    principal,
                    request_id,
                    &request,
                    set_properties_object.as_ref(),
                    &set_properties_probe,
                );
                session.record_recent_probe_action(format!(
                    "{}(in={},kind={},folder={},tags={})",
                    rop_id_hex(request.rop_id),
                    request.input_handle_index().unwrap_or(0),
                    mapi_object_debug_kind(set_properties_object.as_ref()),
                    mapi_object_debug_folder_id(set_properties_object.as_ref()),
                    format_debug_property_tags(&set_properties_probe.property_tags)
                ));
                let values = match request.property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        let response = rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x8004_0102,
                        );
                        let post_hierarchy_contract = post_hierarchy_setprops_contract(
                            &request,
                            set_properties_object.as_ref(),
                            &set_properties_probe,
                            &response,
                        );
                        session.record_post_hierarchy_setprops_contract(
                            post_hierarchy_contract.clone(),
                        );
                        session.record_post_hierarchy_request_contract(format!(
                            "{post_hierarchy_contract}->error"
                        ));
                        responses.extend_from_slice(&response);
                        break;
                    }
                };
                let set_result = if let Some(result) =
                    stage_virtual_conversation_action_property_values(
                        session,
                        &handle_slots,
                        &request,
                        snapshot,
                        values.clone(),
                    ) {
                    result
                } else {
                    match set_properties_object.clone() {
                        Some(MapiObject::Message { .. }) => {
                            stage_message_property_values(session, &handle_slots, &request, values)
                        }
                        Some(
                            object @ (MapiObject::Contact { .. }
                            | MapiObject::Event { .. }
                            | MapiObject::Task { .. }
                            | MapiObject::Note { .. }
                            | MapiObject::JournalEntry { .. }
                            | MapiObject::ConversationAction { .. }
                            | MapiObject::NavigationShortcut { .. }
                            | MapiObject::AssociatedConfig { .. }
                            | MapiObject::DelegateFreeBusyMessage { .. }
                            | MapiObject::PublicFolderItem { .. }
                            | MapiObject::Attachment { .. }),
                        ) => {
                            apply_supported_object_property_values(
                                store, principal, &object, values, mailboxes, emails, snapshot,
                            )
                            .await
                        }
                        object @ Some(MapiObject::Folder { .. }) => {
                            let problems =
                                folder_set_property_problems(object.as_ref(), mailboxes, &values);
                            if !problems.is_empty() {
                                let response =
                                    rop_set_properties_problem_response(&request, &problems);
                                log_set_properties_default_folder_response_debug(
                                    principal,
                                    request_id,
                                    &request,
                                    object.as_ref(),
                                    &set_properties_probe,
                                    &response,
                                );
                                let post_hierarchy_contract = post_hierarchy_setprops_contract(
                                    &request,
                                    object.as_ref(),
                                    &set_properties_probe,
                                    &response,
                                );
                                session.record_post_hierarchy_setprops_contract(
                                    post_hierarchy_contract.clone(),
                                );
                                session.record_post_hierarchy_request_contract(format!(
                                    "{post_hierarchy_contract}->problems"
                                ));
                                responses.extend_from_slice(&response);
                                continue;
                            }
                            record_default_folder_entry_id_aliases(
                                session,
                                object.as_ref(),
                                &values,
                            );
                            let values = default_folder_identification_safe_property_values(
                                principal,
                                object.as_ref(),
                                values,
                            );
                            let result = apply_mapi_property_values(
                                input_object_mut(session, &handle_slots, &request),
                                values.clone(),
                            );
                            if result.is_ok() {
                                if let Some(MapiObject::Folder { folder_id, .. }) = object {
                                    if persist_profile_folder_property_values(
                                        store, principal, folder_id, &values,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        tracing::warn!(
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            folder_id = format_args!("0x{folder_id:016x}"),
                                            property_tags = %format_debug_property_tags(
                                                &values.iter().map(|(tag, _value)| *tag).collect::<Vec<_>>()
                                            ),
                                            "accepted MAPI folder property write but failed to persist profile state"
                                        );
                                    }
                                }
                            }
                            result
                        }
                        _object => apply_mapi_property_values(
                            input_object_mut(session, &handle_slots, &request),
                            values,
                        ),
                    }
                };
                match set_result {
                    Ok(()) => {
                        let response = rop_set_properties_response(&request);
                        log_set_properties_default_folder_response_debug(
                            principal,
                            request_id,
                            &request,
                            set_properties_object.as_ref(),
                            &set_properties_probe,
                            &response,
                        );
                        let post_hierarchy_contract = post_hierarchy_setprops_contract(
                            &request,
                            set_properties_object.as_ref(),
                            &set_properties_probe,
                            &response,
                        );
                        session.record_post_hierarchy_setprops_contract(
                            post_hierarchy_contract.clone(),
                        );
                        session.record_post_hierarchy_request_contract(format!(
                            "{post_hierarchy_contract}->ok"
                        ));
                        responses.extend_from_slice(&response);
                    }
                    Err(_) => {
                        let response = rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x8004_0102,
                        );
                        log_set_properties_default_folder_response_debug(
                            principal,
                            request_id,
                            &request,
                            set_properties_object.as_ref(),
                            &set_properties_probe,
                            &response,
                        );
                        let post_hierarchy_contract = post_hierarchy_setprops_contract(
                            &request,
                            set_properties_object.as_ref(),
                            &set_properties_probe,
                            &response,
                        );
                        session.record_post_hierarchy_setprops_contract(
                            post_hierarchy_contract.clone(),
                        );
                        session.record_post_hierarchy_request_contract(format!(
                            "{post_hierarchy_contract}->error"
                        ));
                        responses.extend_from_slice(&response);
                    }
                }
            }
            Some(RopId::DeleteProperties | RopId::DeletePropertiesNoReplicate) => {
                let property_tags = request.property_tags();
                let object = input_object(session, &handle_slots, &request).cloned();
                let delete_result = if let Some(result) =
                    stage_virtual_conversation_action_property_delete(
                        session,
                        &handle_slots,
                        &request,
                        snapshot,
                        &property_tags,
                    ) {
                    result
                } else if let Some(MapiObject::ConversationAction {
                    folder_id,
                    conversation_action_id,
                    ..
                }) = object
                {
                    delete_conversation_action_properties(
                        store,
                        principal,
                        folder_id,
                        conversation_action_id,
                        snapshot,
                        &property_tags,
                        mailboxes,
                        emails,
                    )
                    .await
                } else if let Some(MapiObject::AssociatedConfig {
                    folder_id,
                    config_id,
                    ..
                }) = object
                {
                    let result = delete_associated_config_properties(
                        store,
                        principal,
                        folder_id,
                        config_id,
                        snapshot,
                        &property_tags,
                    )
                    .await;
                    if let Ok(deleted_property_count) = result {
                        tracing::info!(
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = format_args!("0x{:02x}", request.rop_id),
                            folder_id = format_args!("0x{folder_id:016x}"),
                            config_id = format_args!("0x{config_id:016x}"),
                            property_tags = %format_debug_property_tags(&property_tags),
                            deleted_property_count,
                            "rca debug mapi delete associated config properties"
                        );
                    }
                    result.map(|_| ())
                } else {
                    let custom_delete_result = delete_custom_property_values(
                        store,
                        principal,
                        object.as_ref(),
                        mailboxes,
                        emails,
                        snapshot,
                        &property_tags,
                    )
                    .await;
                    match custom_delete_result {
                        Ok(()) => {
                            let canonical_delete_result = delete_canonical_message_text_properties(
                                store,
                                principal,
                                object.as_ref(),
                                &property_tags,
                                mailboxes,
                                emails,
                                snapshot,
                            )
                            .await;
                            canonical_delete_result.and_then(|_| {
                                delete_mapi_properties(
                                    input_object_mut(session, &handle_slots, &request),
                                    &property_tags,
                                )
                                .or_else(|error| {
                                    if property_tags.iter().all(|tag| is_custom_property_tag(*tag))
                                    {
                                        Ok(())
                                    } else if persisted_message_delete_is_best_effort(
                                        object.as_ref(),
                                    ) {
                                        tracing::info!(
                                            rca_debug = true,
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            request_type = "Execute",
                                            request_rop_id = %format!("{:#04x}", request.rop_id),
                                            object_kind = mapi_object_debug_kind(object.as_ref()),
                                            folder_id = %mapi_object_debug_folder_id(object.as_ref()),
                                            property_tags = %format_debug_property_tags(&property_tags),
                                            delete_error = %error,
                                            fallback_reason = "persisted_message_best_effort_delete",
                                            "rca debug mapi delete properties fallback"
                                        );
                                        Ok(())
                                    } else {
                                        Err(error)
                                    }
                                })
                            })
                        }
                        Err(error) => Err(error),
                    }
                };
                match delete_result {
                    Ok(()) => {
                        responses.extend_from_slice(&rop_delete_properties_response(&request))
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SaveChangesMessage) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if !save_flags_are_supported(&request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let save_changes_object = session.handles.get(&handle).cloned();
                session.record_recent_probe_action(format!(
                    "SaveChangesMessage(in={},handle={},kind={},folder={})",
                    request.input_handle_index().unwrap_or(0),
                    handle,
                    mapi_object_debug_kind(save_changes_object.as_ref()),
                    mapi_object_debug_folder_id(save_changes_object.as_ref())
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x0c",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = handle,
                    object_kind = mapi_object_debug_kind(save_changes_object.as_ref()),
                    folder_id = %mapi_object_debug_folder_id(save_changes_object.as_ref()),
                    save_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
                    "rca debug mapi save changes before inbox probe"
                );
                match session.handles.get(&handle).cloned() {
                    Some(MapiObject::CommonViewNamedView { view_id, .. }) => {
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            view_id,
                        );
                        continue;
                    }
                    Some(MapiObject::PendingContact {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = contact_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_contact_for_mapping(
                                principal.account_id,
                                &folder.collection.id,
                            ),
                            &properties,
                        );
                        match store
                            .create_accessible_contact(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(contact) => {
                                let contact_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Contact,
                                    contact.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(contact_id) => contact_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::Contact,
                                    contact.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::Contact {
                                        folder_id,
                                        contact_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(contact_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    contact_id,
                                );
                            }
                            Err(error) => {
                                let (message_class, subject) =
                                    associated_config_class_and_subject(&properties);
                                let property_tags = properties.keys().copied().collect::<Vec<_>>();
                                tracing::warn!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0c",
                                    folder_id = %format!("{folder_id:#018x}"),
                                    associated_message_class = %message_class,
                                    associated_subject = %subject,
                                    property_tag_count = property_tags.len(),
                                    property_tags = %format_debug_property_tags(&property_tags),
                                    save_error = %error,
                                    "rca debug failed to persist associated config message"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                            }
                        }
                        continue;
                    }
                    Some(MapiObject::PendingEvent {
                        folder_id,
                        properties,
                    }) => {
                        let collection_id = match snapshot.collaboration_folder_for_id(folder_id) {
                            Some(folder) => folder.collection.id.as_str(),
                            None if folder_id == CALENDAR_FOLDER_ID => {
                                DEFAULT_CALENDAR_COLLECTION_ID
                            }
                            None => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        let input = match event_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_event_for_mapping(principal.account_id, collection_id),
                            &properties,
                        ) {
                            Ok(input) => input,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        match store
                            .create_accessible_event(
                                principal.account_id,
                                Some(collection_id),
                                input,
                            )
                            .await
                        {
                            Ok(event) => {
                                let event_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::CalendarEvent,
                                    event.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(event_id) => event_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::CalendarEvent,
                                    event.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::Event {
                                        folder_id,
                                        event_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(event_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    event_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingTask {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = task_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_task_for_mapping(principal.account_id, &folder.collection.id),
                            Some(&folder.collection.id),
                            &properties,
                        );
                        match store
                            .create_accessible_task(principal.account_id, input)
                            .await
                        {
                            Ok(task) => {
                                let task_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Task,
                                    task.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(task_id) => task_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::Task,
                                    task.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session
                                    .handles
                                    .insert(handle, MapiObject::Task { folder_id, task_id });
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(task_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    task_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingNote {
                        folder_id,
                        properties,
                    }) => {
                        let input = note_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_note_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_note(input).await {
                            Ok(note) => {
                                let note_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Note,
                                    note.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(note_id) => note_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::Note,
                                    note.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session
                                    .handles
                                    .insert(handle, MapiObject::Note { folder_id, note_id });
                                record_sync_upload_content_change(
                                    session,
                                    folder_id,
                                    note_id,
                                    mapi_mailstore::change_number_for_store_id(note_id),
                                    false,
                                    true,
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(note_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    note_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingJournalEntry {
                        folder_id,
                        properties,
                    }) => {
                        let input = journal_entry_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_journal_entry_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_journal_entry(input).await {
                            Ok(entry) => {
                                let journal_entry_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::JournalEntry,
                                    entry.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(journal_entry_id) => journal_entry_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::JournalEntry,
                                    entry.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::JournalEntry {
                                        folder_id,
                                        journal_entry_id,
                                    },
                                );
                                record_sync_upload_content_change(
                                    session,
                                    folder_id,
                                    journal_entry_id,
                                    mapi_mailstore::change_number_for_store_id(journal_entry_id),
                                    false,
                                    true,
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(journal_entry_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    journal_entry_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingConversationAction {
                        folder_id,
                        properties,
                    }) => {
                        let action = conversation_action_from_mapi_properties(&properties);
                        if action.conversation_id.is_nil() {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        let move_target_mailbox_id =
                            conversation_action_target_mailbox_id(&action, mailboxes);
                        let input = lpe_storage::UpsertConversationActionInput {
                            account_id: principal.account_id,
                            conversation_id: action.conversation_id,
                            subject: action.subject,
                            categories_json: action.categories_json,
                            move_folder_entry_id: action.move_folder_entry_id,
                            move_store_entry_id: action.move_store_entry_id,
                            move_target_mailbox_id,
                            max_delivery_time: action.max_delivery_time,
                            last_applied_time: action.last_applied_time,
                            version: Some(action.version),
                            processed: Some(action.processed),
                        };
                        match store.upsert_conversation_action(input).await {
                            Ok(saved) => {
                                let conversation_action_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::ConversationAction,
                                    saved.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(conversation_action_id) => conversation_action_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if apply_conversation_action_to_existing_messages(
                                    store, principal, &saved, mailboxes, emails,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::ConversationAction {
                                        folder_id,
                                        conversation_action_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(conversation_action_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    conversation_action_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingNavigationShortcut {
                        folder_id,
                        properties,
                    }) => {
                        let shortcut = navigation_shortcut_from_mapi_properties(
                            principal.account_id,
                            None,
                            &properties,
                        );
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            folder_id = format_args!("0x{:016x}", folder_id),
                            decoded_shortcut =
                                %common_views_saved_shortcut_summary(&shortcut, &properties),
                            "rca debug mapi common views navigation shortcut save"
                        );
                        let input = UpsertMapiNavigationShortcutInput {
                            id: None,
                            account_id: principal.account_id,
                            subject: shortcut.subject,
                            target_folder_id: shortcut.target_folder_id,
                            shortcut_type: shortcut.shortcut_type,
                            flags: shortcut.flags,
                            save_stamp: shortcut.save_stamp,
                            section: shortcut.section,
                            ordinal: shortcut.ordinal,
                            group_header_id: shortcut.group_header_id,
                            group_name: shortcut.group_name,
                        };
                        match store.upsert_mapi_navigation_shortcut(input).await {
                            Ok(saved) => {
                                let shortcut_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::NavigationShortcut,
                                    saved.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Ok(shortcut_id) => shortcut_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::NavigationShortcut {
                                        folder_id,
                                        shortcut_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(shortcut_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    shortcut_id,
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::Event { folder_id, .. })
                        if mapi_calendar_content_items_suppressed(folder_id, snapshot) =>
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                    Some(MapiObject::PendingMessage { .. })
                        if session.pending_embedded_message_ids.contains_key(&handle) =>
                    {
                        let message_id = session
                            .pending_embedded_message_ids
                            .get(&handle)
                            .copied()
                            .unwrap_or(0);
                        if let Some(MapiObject::PendingMessage { properties, .. }) =
                            session.handles.get(&handle).cloned()
                        {
                            if let Some(attachment_key) = session
                                .pending_embedded_message_attachments
                                .get(&handle)
                                .copied()
                            {
                                session
                                    .saved_embedded_messages
                                    .insert(attachment_key, properties);
                            }
                        }
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            message_id,
                        );
                        continue;
                    }
                    Some(MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email,
                        pending_properties,
                    }) => {
                        let staged_property_write = !pending_properties.is_empty();
                        let staged_recipient_replacement = session
                            .pending_message_recipient_replacements
                            .get(&handle)
                            .cloned();
                        let pending = session
                            .pending_attachment_deletions
                            .iter()
                            .filter_map(|(pending_folder_id, pending_message_id, attach_num)| {
                                (*pending_folder_id == folder_id
                                    && *pending_message_id == message_id)
                                    .then_some(*attach_num)
                            })
                            .collect::<Vec<_>>();
                        let has_pending_changes = staged_property_write
                            || staged_recipient_replacement.is_some()
                            || !pending.is_empty();
                        let force_save = request.payload.first().copied().unwrap_or(0) & 0x04 != 0;
                        let current_generation =
                            session.message_save_generation(folder_id, message_id);
                        let handle_generation = session
                            .message_handle_generation(handle)
                            .unwrap_or(current_generation);
                        if has_pending_changes
                            && handle_generation != current_generation
                            && !force_save
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_0109,
                            ));
                            session.handles.insert(
                                handle,
                                MapiObject::Message {
                                    folder_id,
                                    message_id,
                                    saved_email,
                                    pending_properties,
                                },
                            );
                            continue;
                        }
                        if staged_property_write
                            && apply_staged_message_property_values(
                                store,
                                principal,
                                folder_id,
                                message_id,
                                pending_properties.clone(),
                                mailboxes,
                                emails,
                                snapshot,
                            )
                            .await
                            .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            session.handles.insert(
                                handle,
                                MapiObject::Message {
                                    folder_id,
                                    message_id,
                                    saved_email,
                                    pending_properties,
                                },
                            );
                            continue;
                        }
                        if let Some(recipients) = staged_recipient_replacement.as_deref() {
                            if apply_staged_message_recipient_replacement(
                                store, principal, folder_id, message_id, recipients, mailboxes,
                                emails,
                            )
                            .await
                            .is_err()
                            {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                session.handles.insert(
                                    handle,
                                    MapiObject::Message {
                                        folder_id,
                                        message_id,
                                        saved_email,
                                        pending_properties,
                                    },
                                );
                                continue;
                            }
                        }
                        let mut delete_failed = false;
                        for attach_num in pending.iter().copied() {
                            let Some(attachment) =
                                snapshot.attachment_for_message(folder_id, message_id, attach_num)
                            else {
                                session
                                    .pending_attachment_deletions
                                    .remove(&(folder_id, message_id, attach_num));
                                continue;
                            };
                            match store
                                .delete_message_attachment(
                                    principal.account_id,
                                    &attachment.file_reference,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "mapi-delete-attachment".to_string(),
                                        subject: attachment.file_reference.clone(),
                                    },
                                )
                                .await
                            {
                                Ok(Some(_)) => {
                                    session
                                        .pending_attachment_deletions
                                        .remove(&(folder_id, message_id, attach_num));
                                }
                                _ => {
                                    delete_failed = true;
                                    break;
                                }
                            }
                        }
                        if delete_failed {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        if !pending.is_empty() {
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id,
                                Some(message_id),
                            ));
                            record_sync_upload_content_change(
                                session,
                                folder_id,
                                message_id,
                                mapi_mailstore::change_number_for_store_id(message_id),
                                false,
                                true,
                            );
                        }
                        if staged_property_write {
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id,
                                Some(message_id),
                            ));
                        }
                        if staged_recipient_replacement.is_some() {
                            session
                                .pending_message_recipient_replacements
                                .remove(&handle);
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id,
                                Some(message_id),
                            ));
                        }
                        if has_pending_changes {
                            session.record_message_saved(handle, folder_id, message_id);
                        }
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                                saved_email,
                                pending_properties: HashMap::new(),
                            },
                        );
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            message_id,
                        );
                        continue;
                    }
                    Some(MapiObject::Event {
                        folder_id,
                        event_id,
                    }) => {
                        let pending = session
                            .pending_attachment_deletions
                            .iter()
                            .filter_map(|(pending_folder_id, pending_message_id, attach_num)| {
                                (*pending_folder_id == folder_id && *pending_message_id == event_id)
                                    .then_some(*attach_num)
                            })
                            .collect::<Vec<_>>();
                        let mut delete_failed = false;
                        for attach_num in pending.iter().copied() {
                            let Some(attachment) =
                                snapshot.attachment_for_message(folder_id, event_id, attach_num)
                            else {
                                session
                                    .pending_attachment_deletions
                                    .remove(&(folder_id, event_id, attach_num));
                                continue;
                            };
                            match store
                                .delete_calendar_event_attachment(
                                    principal.account_id,
                                    &attachment.file_reference,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "mapi-delete-calendar-attachment".to_string(),
                                        subject: attachment.file_reference.clone(),
                                    },
                                )
                                .await
                            {
                                Ok(Some(_)) => {
                                    session
                                        .pending_attachment_deletions
                                        .remove(&(folder_id, event_id, attach_num));
                                }
                                _ => {
                                    delete_failed = true;
                                    break;
                                }
                            }
                        }
                        if delete_failed {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        if !pending.is_empty() {
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id,
                                Some(event_id),
                            ));
                            record_sync_upload_content_change(
                                session,
                                folder_id,
                                event_id,
                                mapi_mailstore::change_number_for_store_id(event_id),
                                false,
                                true,
                            );
                        }
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            event_id,
                        );
                        continue;
                    }
                    Some(MapiObject::Contact { contact_id, .. })
                    | Some(MapiObject::Task {
                        task_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::Note {
                        note_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::JournalEntry {
                        journal_entry_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::ConversationAction {
                        conversation_action_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::NavigationShortcut {
                        shortcut_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::AssociatedConfig {
                        config_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::DelegateFreeBusyMessage {
                        message_id: contact_id,
                        ..
                    }) => {
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            contact_id,
                        );
                        continue;
                    }
                    Some(MapiObject::PendingAssociatedMessage {
                        folder_id,
                        properties,
                    }) => {
                        match persist_associated_config_message(
                            store,
                            principal,
                            folder_id,
                            &properties,
                        )
                        .await
                        {
                            Ok((saved, message_id)) => {
                                set_handle_slot(
                                    &mut handle_slots,
                                    Some(request.response_handle_index()),
                                    handle,
                                );
                                session.handles.insert(
                                    handle,
                                    MapiObject::AssociatedConfig {
                                        folder_id,
                                        config_id: message_id,
                                        saved_message: Some(
                                            crate::mapi_store::MapiAssociatedConfigMessage {
                                                id: message_id,
                                                folder_id,
                                                canonical_id: saved.id,
                                                message_class: saved.message_class.clone(),
                                                subject: saved.subject.clone(),
                                                properties_json: saved.properties_json.clone(),
                                            },
                                        ),
                                    },
                                );
                                record_sync_upload_content_change(
                                    session,
                                    folder_id,
                                    message_id,
                                    mapi_mailstore::change_number_for_store_id(message_id),
                                    true,
                                    false,
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(message_id),
                                ));
                                append_save_changes_message_response(
                                    &mut responses,
                                    &mut handle_slots,
                                    &request,
                                    handle,
                                    message_id,
                                );
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0c",
                                    folder_id = %format!("{folder_id:#018x}"),
                                    associated_config_id = %saved.id,
                                    mapi_message_id = %format!("{message_id:#018x}"),
                                    "rca debug persisted associated config message"
                                );
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PublicFolderItem {
                        folder_id,
                        item_id,
                        properties,
                    }) => {
                        if !properties.is_empty()
                            && apply_canonical_public_folder_item_property_values(
                                store,
                                principal,
                                folder_id,
                                item_id,
                                properties.into_iter().collect(),
                                snapshot,
                            )
                            .await
                            .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        session.record_notification(MapiNotificationEvent::content(
                            folder_id,
                            Some(item_id),
                        ));
                        record_sync_upload_content_change(
                            session,
                            folder_id,
                            item_id,
                            mapi_mailstore::change_number_for_store_id(item_id),
                            false,
                            true,
                        );
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            item_id,
                        );
                        continue;
                    }
                    _ => {}
                }
                let Some(MapiObject::PendingMessage {
                    folder_id,
                    properties,
                    recipients,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
                    if !recipients.is_empty() {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let input = UpsertPublicFolderItemInput {
                        id: None,
                        account_id: principal.account_id,
                        public_folder_id: folder.folder.id,
                        item_kind: "post".to_string(),
                        message_class: optional_pending_text_property(
                            &properties,
                            &[PID_TAG_MESSAGE_CLASS_W],
                        )
                        .unwrap_or_else(|| "IPM.Post".to_string()),
                        subject: pending_text_property(
                            &properties,
                            &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
                        ),
                        body_text: pending_text_property(&properties, &[PID_TAG_BODY_W]),
                        body_html_sanitized: pending_html_property(&properties),
                        source_payload_json: json!({"source": "mapi-save-message"}).to_string(),
                    };
                    match store
                        .upsert_public_folder_item(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-save-public-folder-item".to_string(),
                                subject: format!("public-folder:{}", folder.folder.id),
                            },
                        )
                        .await
                    {
                        Ok(item) => {
                            let item_id = match remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::PublicFolderItem,
                                item.id,
                                None,
                                None,
                            )
                            .await
                            {
                                Ok(item_id) => item_id,
                                Err(_) => {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                            };
                            session.handles.insert(
                                handle,
                                MapiObject::PublicFolderItem {
                                    folder_id,
                                    item_id,
                                    properties: HashMap::new(),
                                },
                            );
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id,
                                Some(item_id),
                            ));
                            record_sync_upload_content_change(
                                session,
                                folder_id,
                                item_id,
                                mapi_mailstore::change_number_for_store_id(item_id),
                                false,
                                true,
                            );
                            append_save_changes_message_response(
                                &mut responses,
                                &mut handle_slots,
                                &request,
                                handle,
                                item_id,
                            );
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                    continue;
                }
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if pending_message_is_trash_sync_artifact(folder_id, &properties, &recipients) {
                    let message_id = transient_associated_message_id(folder_id, &properties);
                    append_save_changes_message_response(
                        &mut responses,
                        &mut handle_slots,
                        &request,
                        handle,
                        message_id,
                    );
                    continue;
                }
                if pending_message_is_sync_metadata_only(&properties, &recipients) {
                    if folder_id == TRASH_FOLDER_ID {
                        let message_id = transient_associated_message_id(folder_id, &properties);
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                                saved_email: None,
                                pending_properties: HashMap::new(),
                            },
                        );
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            message_id,
                        );
                        continue;
                    }
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x0c",
                        input_handle_index = request.input_handle_index.unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = "pending_message",
                        folder_id = %format!("{folder_id:#018x}"),
                        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                        property_tag_count = properties.len(),
                        property_tags = %format_debug_property_tags(
                            &properties.keys().copied().collect::<Vec<_>>()
                        ),
                        save_rejected_reason = "sync_metadata_only",
                        "rca debug mapi save changes message"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let pending_attachments = session
                    .pending_message_attachments
                    .get(&handle)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(_, attachment)| attachment)
                    .collect::<Vec<_>>();
                let input = jmap_import_from_pending_message(
                    principal,
                    mailbox,
                    &properties,
                    &recipients,
                    pending_attachments,
                );
                let imported_source_key = imported_message_source_key(&properties);
                let imported_source_key_global_counter = imported_source_key
                    .as_deref()
                    .and_then(source_key_global_counter);
                match store
                    .import_jmap_email(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-message".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(email) => {
                        session.pending_message_attachments.remove(&handle);
                        session
                            .pending_attachment_parent_messages
                            .retain(|_, parent_handle| *parent_handle != handle);
                        if apply_conversation_actions_to_new_message(
                            store, principal, mailboxes, &email, snapshot,
                        )
                        .await
                        .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        let (message_id, preserved_import_source_key, identity_fallback_reason) =
                            match remember_created_message_mapi_identity(
                                store,
                                principal,
                                email.id,
                                imported_source_key.clone(),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(error) => {
                                    tracing::info!(
                                        rca_debug = true,
                                        adapter = "mapi",
                                        endpoint = "emsmdb",
                                        mailbox = %principal.email,
                                        request_type = "Execute",
                                        request_rop_id = "0x0c",
                                        input_handle_index = request.input_handle_index.unwrap_or(0),
                                        response_handle_index = request.response_handle_index(),
                                        object_kind = "message",
                                        folder_id = %format!("{folder_id:#018x}"),
                                        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                                        imported_source_key_global_counter = imported_source_key_global_counter
                                            .map(|counter| counter.to_string())
                                            .unwrap_or_default(),
                                        imported_source_key = %imported_source_key
                                            .as_deref()
                                            .map(bytes_to_hex)
                                            .unwrap_or_default(),
                                        identity_error = %error,
                                        "rca debug mapi save changes message identity"
                                    );
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                            };
                        if upsert_custom_property_values_from_map(
                            store,
                            principal,
                            MapiCustomPropertyObjectKind::Message,
                            email.id,
                            &properties,
                        )
                        .await
                        .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                                saved_email: Some(MapiSavedEmail {
                                    email: email.clone(),
                                }),
                                pending_properties: HashMap::new(),
                            },
                        );
                        let associated = matches!(
                            properties.get(&PID_TAG_ASSOCIATED),
                            Some(MapiValue::Bool(true))
                        );
                        record_sync_upload_content_change(
                            session,
                            folder_id,
                            message_id,
                            mapi_mailstore::canonical_message_change_number(&email),
                            associated,
                            !associated,
                        );
                        created_emails.push(email);
                        session.record_notification(MapiNotificationEvent::content(
                            folder_id,
                            Some(message_id),
                        ));
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            input_handle_index = request.input_handle_index.unwrap_or(0),
                            response_handle_index = request.response_handle_index(),
                            object_kind = "message",
                            folder_id = %format!("{folder_id:#018x}"),
                            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                            item_id = %format!("{message_id:#018x}"),
                            imported_source_key_global_counter = imported_source_key_global_counter
                                .map(|counter| counter.to_string())
                                .unwrap_or_default(),
                            imported_source_key = %imported_source_key
                                .as_deref()
                                .map(bytes_to_hex)
                                .unwrap_or_default(),
                            preserved_import_source_key,
                            identity_fallback_reason = %identity_fallback_reason,
                            "rca debug mapi save changes message"
                        );
                        append_save_changes_message_response(
                            &mut responses,
                            &mut handle_slots,
                            &request,
                            handle,
                            message_id,
                        );
                    }
                    Err(error) => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            input_handle_index = request.input_handle_index.unwrap_or(0),
                            response_handle_index = request.response_handle_index(),
                            object_kind = "pending_message",
                            folder_id = %format!("{folder_id:#018x}"),
                            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                            recipient_count = recipients.len(),
                            save_error = %error,
                            "rca debug mapi save changes message"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ))
                    }
                }
            }
            Some(RopId::RemoveAllRecipients) => {
                let input_handle_value = input_handle(&handle_slots, &request);
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::PendingMessage { recipients, .. }) => {
                        recipients.clear();
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    Some(MapiObject::Message { .. }) => {
                        if let Some(handle) = input_handle_value {
                            session
                                .pending_message_recipient_replacements
                                .insert(handle, Vec::new());
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        } else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0D,
                                request.response_handle_index(),
                                0x0000_04B9,
                            ));
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    )),
                }
            }
            Some(RopId::ModifyRecipients) => {
                let input_handle_value = input_handle(&handle_slots, &request);
                match input_object(session, &handle_slots, &request).cloned() {
                    Some(MapiObject::PendingMessage {
                        recipients: existing_recipients,
                        ..
                    }) => {
                        let existing_recipient_count = existing_recipients.len();
                        let address_book_entries = store
                            .fetch_address_book_entries(principal)
                            .await
                            .unwrap_or_default();
                        match request.modify_recipients(principal, &address_book_entries) {
                            Ok(changes) => {
                                let Some(MapiObject::PendingMessage { recipients, .. }) =
                                    input_object_mut(session, &handle_slots, &request)
                                else {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0E,
                                        request.response_handle_index(),
                                        0x0000_04B9,
                                    ));
                                    continue;
                                };
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count = recipients.len(),
                                    recipient_change_count = changes.len(),
                                    recipient_upsert_count = pending_recipient_upsert_count(&changes),
                                    recipient_delete_count = pending_recipient_delete_count(&changes),
                                    recipient_types = %pending_recipient_types_summary(&changes),
                                    recipient_row_ids = %pending_recipient_row_ids_summary(&changes),
                                    parse_error = "",
                                    "rca debug mapi modify recipients"
                                );
                                apply_pending_recipient_changes(recipients, changes);
                                responses.extend_from_slice(&rop_simple_success_response(&request));
                            }
                            Err(error) => {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count,
                                    recipient_payload_bytes = request.payload.len(),
                                    recipient_payload_preview = %hex_preview(&request.payload, 48),
                                    parse_error = %error,
                                    "rca debug mapi modify recipients"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    0x0E,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                            }
                        }
                    }
                    Some(MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email,
                        ..
                    }) => {
                        let Some(handle) = input_handle_value else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0E,
                                request.response_handle_index(),
                                0x0000_04B9,
                            ));
                            continue;
                        };
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                            .or(saved_email.as_ref().map(|saved| &saved.email))
                        else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0E,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let mut recipients = session
                            .pending_message_recipient_replacements
                            .get(&handle)
                            .cloned()
                            .unwrap_or_else(|| pending_recipients_from_email(email));
                        let address_book_entries = store
                            .fetch_address_book_entries(principal)
                            .await
                            .unwrap_or_default();
                        match request.modify_recipients(principal, &address_book_entries) {
                            Ok(changes) => {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count = recipients.len(),
                                    recipient_change_count = changes.len(),
                                    recipient_upsert_count = pending_recipient_upsert_count(&changes),
                                    recipient_delete_count = pending_recipient_delete_count(&changes),
                                    recipient_types = %pending_recipient_types_summary(&changes),
                                    recipient_row_ids = %pending_recipient_row_ids_summary(&changes),
                                    parse_error = "",
                                    "rca debug mapi modify recipients"
                                );
                                apply_pending_recipient_changes(&mut recipients, changes);
                                session
                                    .pending_message_recipient_replacements
                                    .insert(handle, recipients);
                                responses.extend_from_slice(&rop_simple_success_response(&request));
                            }
                            Err(error) => {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count = recipients.len(),
                                    recipient_payload_bytes = request.payload.len(),
                                    recipient_payload_preview = %hex_preview(&request.payload, 48),
                                    parse_error = %error,
                                    "rca debug mapi modify recipients"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    0x0E,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                            }
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0E,
                        request.response_handle_index(),
                        0x0000_04B9,
                    )),
                }
            }
            Some(RopId::ReadRecipients) => {
                if request.read_recipients_reserved() != Some(0) {
                    responses.extend_from_slice(&rop_error_response(
                        0x0F,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let input_handle_value = input_handle(&handle_slots, &request);
                let pending_recipient_object;
                let object = if let Some(recipients) = input_handle_value
                    .and_then(|handle| session.pending_message_recipient_replacements.get(&handle))
                {
                    let folder_id = input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID);
                    pending_recipient_object = MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: recipients.clone(),
                    };
                    Some(&pending_recipient_object)
                } else {
                    input_object(session, &handle_slots, &request)
                };
                responses.extend_from_slice(&rop_read_recipients_response(
                    &request, object, mailboxes, emails, snapshot,
                ))
            }
            Some(RopId::ReloadCachedInformation) => {
                if request.reload_cached_information_reserved() != Some(0) {
                    responses.extend_from_slice(&rop_error_response(
                        0x10,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_reload_cached_information_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SetMessageReadFlag) => {
                let Some(object) = input_object(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                if !read_flags_are_valid(request.read_flags(), false) {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                if let MapiObject::PublicFolderItem {
                    folder_id, item_id, ..
                } = object
                {
                    let Some(item) = snapshot.public_folder_item_for_id(*folder_id, *item_id)
                    else {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    };
                    let unread = unread_from_read_flags(request.read_flags());
                    let changed = unread.is_some_and(|unread| unread == item.item.is_read);
                    if let Some(unread) = unread {
                        let patch = lpe_storage::PublicFolderPerUserStatePatch {
                            item_id: item.item.id,
                            is_read: !unread,
                            last_seen_change: Some(item.item.change_counter),
                            private_json: None,
                        };
                        if store
                            .patch_public_folder_per_user_state(
                                principal.account_id,
                                item.item.public_folder_id,
                                &[patch],
                            )
                            .await
                            .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x11,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                    }
                    if changed {
                        session
                            .record_notification(MapiNotificationEvent::content(*folder_id, None));
                    }
                    responses
                        .extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
                    continue;
                }
                let MapiObject::Message {
                    folder_id,
                    message_id,
                    saved_email,
                    ..
                } = object
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails)
                    .or(saved_email.as_ref().map(|saved| &saved.email))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let unread = unread_from_read_flags(request.read_flags());
                let changed = unread.is_some_and(|unread| unread != email.unread);
                if let Some(unread) = unread {
                    if !snapshot
                        .folder_access_for_principal(*folder_id, principal.account_id)
                        .map(|access| access.may_write)
                        .unwrap_or(true)
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-set-message-read-flag".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                }
                if changed {
                    session.record_notification(MapiNotificationEvent::content(*folder_id, None));
                }
                responses.extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
            }
            Some(RopId::SetColumns) => {
                let requested_columns = request.property_tags();
                let normalized_columns =
                    normalize_table_property_tags_for_session(session, requested_columns.clone());
                let input_handle_value = input_handle(&handle_slots, &request);
                let normalized_named_property_context = (requested_columns != normalized_columns)
                    .then(|| format_debug_named_property_context(session, &requested_columns))
                    .unwrap_or_default();
                let selected_named_property_context =
                    format_debug_named_property_context(session, &normalized_columns);
                let mut inbox_normal_setcolumns_context = None;
                let mut calendar_normal_setcolumns_context = None;
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::HierarchyTable {
                        folder_id,
                        columns,
                        columns_set,
                        ..
                    }) => {
                        if !set_columns_request_is_valid(&request) {
                            columns.clear();
                            *columns_set = false;
                            responses.extend_from_slice(&rop_error_response(
                                0x12,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            break;
                        }
                        let folder_id_value = *folder_id;
                        *columns = normalized_columns.clone();
                        *columns_set = true;
                        let selected_columns = columns.clone();
                        if folder_id_value == INBOX_FOLDER_ID {
                            session.record_last_inbox_hierarchy_table_context(format!(
                                "set_columns_input_index={};set_columns={}",
                                request.input_handle_index().unwrap_or(0),
                                format_debug_property_tags(&selected_columns)
                            ));
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x12",
                                folder_id = %format!("0x{folder_id_value:016x}"),
                                input_handle_index = request.input_handle_index().unwrap_or(0),
                                requested_columns = %format_debug_property_tags(&selected_columns),
                                message = "rca debug mapi inbox hierarchy set columns"
                            );
                        }
                        responses.extend_from_slice(&set_columns_response(&request));
                    }
                    Some(MapiObject::AttachmentTable {
                        columns,
                        columns_set,
                        ..
                    }) => {
                        if !set_columns_request_is_valid(&request) {
                            columns.clear();
                            *columns_set = false;
                            responses.extend_from_slice(&rop_error_response(
                                0x12,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            break;
                        }
                        *columns = normalized_columns.clone();
                        *columns_set = true;
                        responses.extend_from_slice(&set_columns_response(&request));
                    }
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        columns_set,
                        ..
                    }) => {
                        if !set_columns_request_is_valid(&request) {
                            tracing::warn!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x12",
                                folder_id = %format!("0x{folder_id:016x}"),
                                associated = *associated,
                                requested_columns = %format_debug_property_tags(&request.property_tags()),
                                unknown_wire_type_columns =
                                    %format_unknown_wire_type_property_tags(&request.property_tags()),
                                response_error = "0x80070057",
                                message = "rca debug mapi contents table set columns rejected",
                            );
                            columns.clear();
                            *columns_set = false;
                            responses.extend_from_slice(&rop_error_response(
                                0x12,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            break;
                        }
                        *columns = normalized_columns.clone();
                        *columns_set = true;
                        if !normalized_named_property_context.is_empty() {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x12",
                                folder_id = %format!("0x{folder_id:016x}"),
                                associated = *associated,
                                requested_columns = %format_debug_property_tags(&requested_columns),
                                normalized_columns = %format_debug_property_tags(columns),
                                named_property_context = %normalized_named_property_context,
                                message = "rca debug mapi contents table set columns normalized named property aliases",
                            );
                        }
                        log_outlook_contents_table_set_columns(
                            principal,
                            request_id,
                            &request,
                            *folder_id,
                            *associated,
                            columns,
                            &selected_named_property_context,
                            snapshot,
                        );
                        if *folder_id == INBOX_FOLDER_ID && !*associated {
                            let row_count =
                                folder_message_count(*folder_id, mailboxes, emails, snapshot);
                            let view_handoff_table_contract =
                                format_outlook_view_handoff_table_contract(
                                    *folder_id,
                                    *associated,
                                    columns,
                                    snapshot,
                                );
                            let descriptor_behavior =
                                format_inbox_view_descriptor_set_columns_behavior_contract(
                                    *folder_id,
                                    *associated,
                                    columns,
                                    snapshot,
                                );
                            inbox_normal_setcolumns_context = Some((
                                input_handle_value,
                                format!(
                                    "handle={};input_index={};row_count={};columns={};column_support={};normal_message_defaulted_column_detail={};named_properties={};view_handoff={};descriptor_behavior={}",
                                    format_optional_debug_handle(input_handle_value),
                                    request.input_handle_index().unwrap_or(0),
                                    row_count,
                                    format_debug_property_tags(columns),
                                    normal_message_table_column_support_summary(columns),
                                    normal_message_defaulted_column_detail(columns),
                                    selected_named_property_context,
                                    view_handoff_table_contract,
                                    descriptor_behavior
                                ),
                            ));
                        }
                        if *folder_id == CALENDAR_FOLDER_ID && !*associated {
                            let row_count =
                                folder_message_count(*folder_id, mailboxes, emails, snapshot);
                            calendar_normal_setcolumns_context = Some(format!(
                                "handle={};input_index={};row_count={};columns={};named_properties={};view_handoff={}",
                                format_optional_debug_handle(input_handle_value),
                                request.input_handle_index().unwrap_or(0),
                                row_count,
                                format_debug_property_tags(columns),
                                selected_named_property_context,
                                format_outlook_view_handoff_table_contract(
                                    *folder_id,
                                    *associated,
                                    columns,
                                    snapshot,
                                )
                            ));
                        }
                        responses.extend_from_slice(&set_columns_response(&request));
                    }
                    Some(MapiObject::PermissionTable {
                        columns,
                        columns_set,
                        ..
                    }) => {
                        if !set_columns_request_is_valid(&request) {
                            columns.clear();
                            *columns_set = false;
                            responses.extend_from_slice(&rop_error_response(
                                0x12,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            break;
                        }
                        *columns = normalized_columns.clone();
                        *columns_set = true;
                        responses.extend_from_slice(&set_columns_response(&request));
                    }
                    Some(MapiObject::RuleTable {
                        columns,
                        columns_set,
                        ..
                    }) => {
                        if !set_columns_request_is_valid_for_rule_table(&request) {
                            tracing::warn!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x12",
                                requested_columns = %format_debug_property_tags(&request.property_tags()),
                                unknown_wire_type_columns =
                                    %format_unknown_wire_type_property_tags(&request.property_tags()),
                                response_error = "0x80070057",
                                message = "rca debug mapi rule table set columns rejected",
                            );
                            columns.clear();
                            *columns_set = false;
                            responses.extend_from_slice(&rop_error_response(
                                0x12,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            break;
                        }
                        *columns = normalized_columns.clone();
                        *columns_set = true;
                        responses.extend_from_slice(&set_columns_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x12,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
                if let Some((handle, context)) = inbox_normal_setcolumns_context {
                    session.record_inbox_normal_contents_table_setcolumns(handle, context.clone());
                    session.record_outlook_view_failure_trace_event(format!(
                        "visible_inbox_setcolumns:{context}"
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %request_id,
                        request_rop_id = "0x12",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(handle),
                        setcolumns_context = %context,
                        "rca debug mapi visible inbox setcolumns tracked"
                    );
                }
                if let Some(context) = calendar_normal_setcolumns_context {
                    session.record_outlook_view_failure_trace_event(format!(
                        "calendar_normal_setcolumns:{context}"
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x12",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        setcolumns_context = %context,
                        "rca debug mapi calendar normal setcolumns tracked"
                    );
                }
            }
            Some(RopId::SortTable) => {
                let sort_trace = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID => Some(format!(
                        "inbox_sort_table:request_id={request_id};handle={};associated={associated};columns={};sort={}",
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(&request.sort_orders())
                    )),
                    _ => None,
                };
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::ContentsTable {
                        sort_orders,
                        category_count,
                        expanded_count,
                        collapsed_categories,
                        position,
                        bookmarks,
                        ..
                    }) => {
                        if !sort_table_request_is_valid(&request) {
                            *sort_orders = invalid_table_sort_orders();
                            *category_count = 0;
                            *expanded_count = 0;
                            collapsed_categories.clear();
                            *position = 0;
                            bookmarks.clear();
                            responses.extend_from_slice(&rop_error_response(
                                0x13,
                                request.response_handle_index(),
                                0x8007_0057,
                            ));
                            if let Some(trace) = sort_trace {
                                session.record_outlook_view_failure_trace_event(trace);
                            }
                            continue;
                        }
                        *sort_orders = request.sort_orders();
                        *category_count = request.sort_category_count();
                        *expanded_count = request.sort_expanded_count();
                        collapsed_categories.clear();
                        *position = 0;
                        bookmarks.clear();
                        let selected_named_property_context =
                            format_contents_table_named_property_context(
                                session,
                                input_object(session, &handle_slots, &request),
                            );
                        log_outlook_contents_table_sort(
                            principal,
                            &request,
                            input_object(session, &handle_slots, &request),
                            &selected_named_property_context,
                            snapshot,
                        );
                        responses.extend_from_slice(&sort_table_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x13,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
                if let Some(trace) = sort_trace {
                    session.record_outlook_view_failure_trace_event(trace);
                }
            }
            Some(RopId::Restrict) => {
                if !input_object(session, &handle_slots, &request)
                    .is_some_and(restrict_supported_on_object)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if !table_async_flags_are_valid(&request) {
                    if let Some(
                        MapiObject::HierarchyTable {
                            restriction,
                            position,
                            bookmarks,
                            ..
                        }
                        | MapiObject::ContentsTable {
                            restriction,
                            position,
                            bookmarks,
                            ..
                        },
                    ) = input_object_mut(session, &handle_slots, &request)
                    {
                        *restriction = Some(MapiRestriction::InvalidTableRestriction);
                        *position = 0;
                        bookmarks.clear();
                    }
                    responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let restrict_trace = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID => Some(format!(
                        "inbox_restrict:request_id={request_id};handle={};associated={associated};columns={};restriction_tags={}",
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        format_debug_property_tags(columns),
                        request
                            .restriction()
                            .ok()
                            .and_then(|restriction| restriction)
                            .map(|restriction| {
                                format_debug_restriction_property_tags(Some(&restriction))
                            })
                            .unwrap_or_default()
                    )),
                    _ => None,
                };
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::HierarchyTable {
                        restriction,
                        position,
                        bookmarks,
                        ..
                    })
                    | Some(MapiObject::ContentsTable {
                        restriction,
                        position,
                        bookmarks,
                        ..
                    }) => match request.restriction() {
                        Ok(parsed) => {
                            *restriction = parsed;
                            *position = 0;
                            bookmarks.clear();
                            let selected_named_property_context =
                                format_contents_table_named_property_context(
                                    session,
                                    input_object(session, &handle_slots, &request),
                                );
                            log_outlook_contents_table_restrict(
                                principal,
                                &request,
                                input_object(session, &handle_slots, &request),
                                &selected_named_property_context,
                                snapshot,
                            );
                            responses.extend_from_slice(&restrict_response(&request));
                        }
                        Err(_) => {
                            *restriction = Some(MapiRestriction::InvalidTableRestriction);
                            *position = 0;
                            bookmarks.clear();
                            responses.extend_from_slice(&rop_error_response(
                                0x14,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            break;
                        }
                    },
                    Some(MapiObject::RuleTable { position, .. }) => {
                        *position = 0;
                        responses.extend_from_slice(&restrict_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
                if let Some(trace) = restrict_trace {
                    session.record_outlook_view_failure_trace_event(trace);
                }
            }
            Some(RopId::QueryRows) => {
                let input_handle_value = input_handle(&handle_slots, &request);
                let query_object = input_object(session, &handle_slots, &request);
                let inbox_normal_query_rows_context = match query_object {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID && !*associated => Some((
                        input_handle_value,
                        format!(
                            "handle={};input_index={};position={};requested_forward_read={};requested_row_count={};columns={};column_support={};sort={};restriction={}",
                            format_optional_debug_handle(input_handle_value),
                            request.input_handle_index().unwrap_or(0),
                            position,
                            request.query_forward_read(),
                            request.query_row_count().unwrap_or(0),
                            format_debug_property_tags(columns),
                            normal_message_table_column_support_summary(columns),
                            format_debug_sort_orders(sort_orders),
                            format_debug_restriction_option(restriction.as_ref())
                        ),
                    )),
                    _ => None,
                };
                let calendar_normal_query_rows_context = match query_object {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => Some(format!(
                        "handle={};input_index={};position={};requested_forward_read={};requested_row_count={};columns={};sort={};restriction={}",
                        format_optional_debug_handle(input_handle_value),
                        request.input_handle_index().unwrap_or(0),
                        position,
                        request.query_forward_read(),
                        request.query_row_count().unwrap_or(0),
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(sort_orders),
                        format_debug_restriction_option(restriction.as_ref())
                    )),
                    _ => None,
                };
                let bootstrap_query_phase = outlook_bootstrap_query_rows_phase(query_object);
                let bootstrap_row_invariants = outlook_bootstrap_row_invariant_summaries(
                    query_object,
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                    request.query_forward_read(),
                    request.query_row_count().unwrap_or(0),
                );
                let bootstrap_total_row_count = outlook_bootstrap_query_rows_total_count(
                    query_object,
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                );
                let selected_named_property_context =
                    format_contents_table_named_property_context(session, query_object);
                log_calendar_hierarchy_query_rows_contract(principal, query_object, snapshot);
                log_outlook_contents_table_query_rows(
                    principal,
                    request_id,
                    &request,
                    query_object,
                    mailboxes,
                    emails,
                    &selected_named_property_context,
                    snapshot,
                );
                let inbox_associated_query_context = format_inbox_associated_query_context(
                    input_object(session, &handle_slots, &request),
                    &request,
                    principal.account_id,
                    snapshot,
                );
                let common_views_inbox_shortcut_context =
                    format_common_views_inbox_shortcut_context(
                        query_object,
                        &request,
                        principal.account_id,
                        snapshot,
                    );
                let inbox_hierarchy_query_context = format_inbox_hierarchy_query_context(
                    query_object,
                    &request,
                    mailboxes,
                    snapshot,
                );
                if let Some(context) = inbox_associated_query_context {
                    session.record_last_inbox_associated_query_context(context);
                }
                if let Some(context) = common_views_inbox_shortcut_context {
                    session.record_last_common_views_inbox_shortcut_context(context);
                }
                if let Some(context) = inbox_hierarchy_query_context {
                    session.record_last_inbox_hierarchy_query_context(context.clone());
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x15",
                        folder_id = %format!("0x{INBOX_FOLDER_ID:016x}"),
                        query_context = %context,
                        message = "rca debug mapi inbox hierarchy query rows"
                    );
                }
                let smart_input_variant_context =
                    apply_outlook_smart_input_variant_before_query_rows(
                        session,
                        &handle_slots,
                        &request,
                        request_id,
                        &request_rop_names,
                    );
                if let Some(context) = smart_input_variant_context {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %request_id,
                        request_rop_id = "0x15",
                        outlook_smart_input_variant = %session.outlook_smart_input_variant,
                        outlook_smart_input_variant_scope = "session",
                        outlook_smart_input_variant_applied = true,
                        outlook_smart_input_variant_context = %context,
                        message = "rca debug mapi outlook smart input variant applied"
                    );
                }
                let queried_position = input_object(session, &handle_slots, &request)
                    .and_then(table_position)
                    .unwrap_or(0);
                let response = query_rows_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                );
                log_outlook_contents_table_query_rows_response(
                    principal,
                    request_id,
                    &request,
                    input_object(session, &handle_slots, &request),
                    &response,
                    snapshot,
                    &selected_named_property_context,
                    queried_position,
                );
                log_outlook_hierarchy_table_query_rows_response(
                    principal,
                    request_id,
                    &request,
                    input_object(session, &handle_slots, &request),
                    &response,
                    mailboxes,
                    emails,
                    snapshot,
                    queried_position,
                );
                let mut inbox_associated_query_rows_returned_non_empty = false;
                if let Some(MapiObject::ContentsTable {
                    folder_id,
                    associated,
                    columns,
                    position,
                    restriction,
                    sort_orders,
                    ..
                }) = input_object(session, &handle_slots, &request)
                {
                    let row_count = response
                        .get(7..9)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_le_bytes)
                        .unwrap_or(0);
                    if *folder_id == INBOX_FOLDER_ID && *associated && row_count > 0 {
                        inbox_associated_query_rows_returned_non_empty = true;
                    }
                    session.record_last_table_query_rows_context(format!(
                        "phase=query_rows;request_id={request_id};request_rops={request_rop_names};input_index={};handle={};folder=0x{folder_id:016x};role={};associated={associated};queried_position={queried_position};current_position_after={position};requested_forward_read={};requested_row_count={};response_row_count={row_count};columns={};sort={};restriction={}",
                        request.input_handle_index().unwrap_or(0),
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        debug_role_for_folder_id(*folder_id),
                        request.query_forward_read(),
                        request.query_row_count().unwrap_or(0),
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(sort_orders),
                        format_debug_restriction_option(restriction.as_ref())
                    ));
                }
                if inbox_associated_query_rows_returned_non_empty {
                    session.record_inbox_associated_query_rows_returned_non_empty();
                }
                responses.extend_from_slice(&response);
                if let Some((phase, folder_id, associated)) = bootstrap_query_phase {
                    log_outlook_bootstrap_phase(
                        principal,
                        phase,
                        "0x15",
                        Some(folder_id),
                        associated,
                        bootstrap_total_row_count,
                        Some(bootstrap_row_invariants.len() as u32),
                        None,
                        "",
                    );
                    for summary in bootstrap_row_invariants {
                        log_outlook_bootstrap_row_invariant(
                            principal, phase, folder_id, associated, &summary,
                        );
                    }
                }
                if let Some((handle, context)) = inbox_normal_query_rows_context {
                    session.record_inbox_normal_contents_table_query_rows(handle, context.clone());
                    session.record_outlook_view_failure_trace_event(format!(
                        "visible_inbox_query_rows:{context}"
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %request_id,
                        request_rop_id = "0x15",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(handle),
                        query_rows_context = %context,
                        "rca debug mapi visible inbox query rows tracked"
                    );
                }
                if let Some(context) = calendar_normal_query_rows_context {
                    session.record_outlook_view_failure_trace_event(format!(
                        "calendar_normal_query_rows:{context}"
                    ));
                }
            }
            Some(RopId::GetStatus) => responses.extend_from_slice(&get_status_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            Some(RopId::QueryPosition) => {
                let calendar_normal_query_position_context = match input_object(
                    session,
                    &handle_slots,
                    &request,
                ) {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        sort_orders,
                        ..
                    }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => Some(format!(
                        "handle={};input_index={};position_before={};columns={};sort={};restriction={}",
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        request.input_handle_index().unwrap_or(0),
                        position,
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(sort_orders),
                        format_debug_restriction_option(restriction.as_ref())
                    )),
                    _ => None,
                };
                let response = query_position_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                );
                log_mapi_query_position_debug(
                    principal,
                    request_id,
                    &request,
                    input_object(session, &handle_slots, &request),
                    &response,
                    mailboxes,
                    emails,
                    snapshot,
                );
                if let Some(context) = calendar_normal_query_position_context {
                    let position = response
                        .get(6..10)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u32::from_le_bytes)
                        .unwrap_or(0);
                    let row_count = response
                        .get(10..14)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u32::from_le_bytes)
                        .unwrap_or(0);
                    session.record_outlook_view_failure_trace_event(format!(
                        "calendar_normal_query_position:{context};response_position={position};response_row_count={row_count}"
                    ));
                }
                responses.extend_from_slice(&response);
            }
            Some(RopId::SeekRow) => {
                let before_position =
                    input_object(session, &handle_slots, &request).and_then(table_position);
                let selected_named_property_context = format_contents_table_named_property_context(
                    session,
                    input_object(session, &handle_slots, &request),
                );
                let response = seek_row_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                );
                log_outlook_contents_table_seek_row(
                    principal,
                    &request,
                    input_object(session, &handle_slots, &request),
                    &selected_named_property_context,
                    snapshot,
                    before_position,
                    &response,
                );
                responses.extend_from_slice(&response);
            }
            Some(RopId::SeekRowBookmark) => {
                responses.extend_from_slice(&seek_row_bookmark_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                ))
            }
            Some(RopId::SeekRowFractional) => {
                responses.extend_from_slice(&seek_row_fractional_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                ))
            }
            Some(RopId::CreateBookmark) => responses.extend_from_slice(&create_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
                principal.account_id,
            )),
            Some(RopId::QueryColumnsAll) => {
                responses.extend_from_slice(&query_columns_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                ))
            }
            Some(RopId::ExpandRow)
                if !matches!(
                    input_object(session, &handle_slots, &request),
                    Some(MapiObject::Folder { .. })
                ) =>
            {
                responses.extend_from_slice(&expand_row_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::CollapseRow) => responses.extend_from_slice(&collapse_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::GetCollapseState) => {
                responses.extend_from_slice(&get_collapse_state_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ))
            }
            Some(RopId::SetCollapseState) => {
                responses.extend_from_slice(&set_collapse_state_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                ))
            }
            Some(RopId::CreateFolder) => {
                let parent_folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let parent_mailbox = folder_row_for_id(parent_folder_id, mailboxes);
                let parent_public_folder_id = snapshot
                    .public_folder_for_id(parent_folder_id)
                    .map(|folder| folder.folder.id);
                if !is_root_hierarchy_folder(parent_folder_id)
                    && parent_mailbox.is_none()
                    && parent_public_folder_id.is_none()
                    && parent_folder_id != SEARCH_FOLDER_ID
                    && role_for_folder_id(parent_folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let create_parent_id = parent_mailbox.map(|mailbox| mailbox.id);
                let display_name = request.create_folder_display_name();
                let display_name = display_name.trim();
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x1c",
                    parent_folder_id = %format!("{parent_folder_id:#018x}"),
                    folder_type = request.create_folder_type(),
                    open_existing = request.create_folder_open_existing(),
                    display_name = display_name,
                    message = "rca debug mapi create folder request",
                );
                if display_name.is_empty()
                    || !matches!(request.create_folder_type(), 1 | 2)
                    || request.create_folder_reserved() != 0
                {
                    tracing::warn!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        tenant_id = %principal.tenant_id,
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x1c",
                        parent_folder_id = %format!("{parent_folder_id:#018x}"),
                        folder_type = request.create_folder_type(),
                        open_existing = request.create_folder_open_existing(),
                        reserved = request.create_folder_reserved(),
                        display_name = display_name,
                        response_error = "0x80070057",
                        message = "rca debug mapi create folder invalid request",
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0057,
                    ));
                    continue;
                }

                if let Some(folder_id) =
                    advertised_special_folder_id_for_create(parent_folder_id, display_name)
                {
                    if session.advertised_special_folder_was_deleted(folder_id) {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            tenant_id = %principal.tenant_id,
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x1c",
                            parent_folder_id = %format!("{parent_folder_id:#018x}"),
                            folder_type = request.create_folder_type(),
                            open_existing = request.create_folder_open_existing(),
                            display_name = display_name,
                            deleted_advertised_folder_id = %format!("0x{folder_id:016x}"),
                            message = "rca debug mapi create folder skipped deleted advertised special folder",
                        );
                    } else {
                        let requested_open_existing = request.create_folder_open_existing();
                        let response_existing = private_create_folder_is_existing_response_flag();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            tenant_id = %principal.tenant_id,
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x1c",
                            parent_folder_id = %format!("{parent_folder_id:#018x}"),
                            folder_type = request.create_folder_type(),
                            open_existing = requested_open_existing,
                            display_name = display_name,
                            matched_advertised_folder_id = %format!("0x{folder_id:016x}"),
                            response_existing_folder = response_existing,
                            message = "rca debug mapi create folder opened advertised special folder",
                        );
                        let properties = folder_properties_for_open(
                            store, principal, session, folder_id, mailboxes, snapshot,
                        )
                        .await;
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request,
                            folder_id,
                            response_existing,
                        ));
                        if !requested_open_existing {
                            session.record_notification(MapiNotificationEvent::hierarchy(
                                parent_folder_id,
                                Some(folder_id),
                            ));
                        }
                        output_handles.push(handle);
                        continue;
                    }
                }

                if let Some(parent_public_folder_id) = parent_public_folder_id {
                    if request.create_folder_type() != 1 {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x8000_4005,
                        ));
                        continue;
                    }
                    let existing_public_folder_id = snapshot
                        .public_folders()
                        .iter()
                        .find(|folder| {
                            folder.folder.parent_folder_id == Some(parent_public_folder_id)
                                && folder.folder.lifecycle_state == "active"
                                && folder
                                    .folder
                                    .display_name
                                    .eq_ignore_ascii_case(display_name)
                        })
                        .map(|folder| folder.folder.id);
                    if let Some(existing_public_folder_id) = existing_public_folder_id {
                        if !request.create_folder_open_existing() {
                            responses.extend_from_slice(&rop_error_response(
                                0x1C,
                                request.output_handle_index.unwrap_or(0),
                                0x8004_0604,
                            ));
                            continue;
                        }
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::PublicFolder,
                            existing_public_folder_id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        let properties = folder_properties_for_open(
                            store, principal, session, folder_id, mailboxes, snapshot,
                        )
                        .await;
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        output_handles.push(handle);
                        continue;
                    }

                    match store
                        .create_public_folder_child(
                            CreatePublicFolderInput {
                                account_id: principal.account_id,
                                parent_folder_id: parent_public_folder_id,
                                display_name: display_name.to_string(),
                                folder_class: "IPF.Note".to_string(),
                                sort_order: 0,
                            },
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-create-public-folder".to_string(),
                                subject: display_name.to_string(),
                            },
                        )
                        .await
                    {
                        Ok(folder) => {
                            let folder_id = match remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::PublicFolder,
                                folder.id,
                                None,
                                None,
                            )
                            .await
                            {
                                Ok(folder_id) => folder_id,
                                Err(_) => {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x1C,
                                        request.output_handle_index.unwrap_or(0),
                                        0x8004_0102,
                                    ));
                                    continue;
                                }
                            };
                            let properties = public_folder_handle_properties(&folder, folder_id);
                            let handle = session.allocate_output_handle(
                                request.output_handle_index,
                                MapiObject::Folder {
                                    folder_id,
                                    properties,
                                },
                            );
                            set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                            responses.extend_from_slice(&rop_create_folder_response(
                                &request, folder_id, false,
                            ));
                            session.record_notification(MapiNotificationEvent::hierarchy(
                                parent_folder_id,
                                Some(folder_id),
                            ));
                            output_handles.push(handle);
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x8007_0005,
                        )),
                    }
                    continue;
                }

                if parent_folder_id == SEARCH_FOLDER_ID
                    || request.create_folder_type() == FOLDER_SEARCH as u8
                {
                    if let Some(definition) = snapshot
                        .user_saved_search_folder_definition_by_display_name(
                            display_name,
                            "message",
                        )
                    {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::SearchFolderDefinition,
                            definition.id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            tenant_id = %principal.tenant_id,
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x1c",
                            parent_folder_id = %format!("{parent_folder_id:#018x}"),
                            folder_id = %format!("{folder_id:#018x}"),
                            search_folder_id = %definition.id,
                            folder_type = request.create_folder_type(),
                            open_existing = request.create_folder_open_existing(),
                            display_name = display_name,
                            reused_existing_search_folder = true,
                            message = "rca debug mapi create folder reused search folder",
                        );
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties: search_folder_handle_properties(
                                    definition,
                                    folder_id,
                                    principal.account_id,
                                ),
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request,
                            folder_id,
                            private_create_folder_is_existing_response_flag(),
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                    let definition_id = Uuid::new_v4();
                    let folder_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::SearchFolderDefinition,
                        definition_id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(folder_id) => folder_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x1C,
                                request.output_handle_index.unwrap_or(0),
                                0x8004_0102,
                            ));
                            continue;
                        }
                    };
                    let definition = SearchFolderDefinition {
                        id: definition_id,
                        account_id: principal.account_id,
                        role: "custom".to_string(),
                        display_name: display_name.to_string(),
                        definition_kind: "user_saved".to_string(),
                        result_object_kind: "message".to_string(),
                        scope_json: json!({
                            "kind": "mapi_bounded",
                            "scope": "folders",
                            "recursive": true,
                            "folderIds": [],
                            "folderRoles": ["inbox"]
                        }),
                        restriction_json: json!({
                            "kind": "mapi_bounded",
                            "all": []
                        }),
                        excluded_folder_roles: Vec::new(),
                        is_builtin: false,
                    };
                    session.remember_search_folder_definition(folder_id, definition.clone());
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        tenant_id = %principal.tenant_id,
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x1c",
                        parent_folder_id = %format!("{parent_folder_id:#018x}"),
                        folder_id = %format!("{folder_id:#018x}"),
                        search_folder_id = %definition.id,
                        folder_type = request.create_folder_type(),
                        open_existing = request.create_folder_open_existing(),
                        display_name = display_name,
                        message = "rca debug mapi create folder staged search folder",
                    );
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Folder {
                            folder_id,
                            properties: search_folder_handle_properties(
                                &definition,
                                folder_id,
                                principal.account_id,
                            ),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses
                        .extend_from_slice(&rop_create_folder_response(&request, folder_id, false));
                    output_handles.push(handle);
                    continue;
                }

                let existing_mailbox = mailboxes.iter().find(|mailbox| {
                    mailbox.parent_id == create_parent_id
                        && mailbox.name.eq_ignore_ascii_case(display_name)
                });
                let deleted_advertised_existing =
                    create_folder_existing_mailbox_satisfies_deleted_advertised_request(
                        session,
                        parent_folder_id,
                        display_name,
                    );
                if request.create_folder_open_existing() || deleted_advertised_existing {
                    if let Some(existing) = existing_mailbox {
                        let folder_id = mapi_folder_id(existing);
                        if deleted_advertised_existing && !request.create_folder_open_existing() {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                tenant_id = %principal.tenant_id,
                                account_id = %principal.account_id,
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = "0x1c",
                                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                                folder_id = %format!("{folder_id:#018x}"),
                                folder_type = request.create_folder_type(),
                                open_existing = request.create_folder_open_existing(),
                                display_name = display_name,
                                response_existing_folder = false,
                                message = "rca debug mapi create folder opened real folder replacing deleted advertised folder",
                            );
                        }
                        let properties = folder_properties_for_open(
                            store, principal, session, folder_id, mailboxes, snapshot,
                        )
                        .await;
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request,
                            folder_id,
                            if deleted_advertised_existing {
                                false
                            } else {
                                private_create_folder_is_existing_response_flag()
                            },
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                } else if existing_mailbox.is_some() {
                    tracing::warn!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        tenant_id = %principal.tenant_id,
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x1c",
                        parent_folder_id = %format!("{parent_folder_id:#018x}"),
                        folder_type = request.create_folder_type(),
                        open_existing = request.create_folder_open_existing(),
                        display_name = display_name,
                        response_error = "0x80040604",
                        message = "rca debug mapi create folder duplicate name",
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0604,
                    ));
                    continue;
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.to_string(),
                            parent_id: create_parent_id,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-create-folder".to_string(),
                            subject: display_name.to_string(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            tenant_id = %principal.tenant_id,
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x1c",
                            parent_folder_id = %format!("{parent_folder_id:#018x}"),
                            folder_id = %format!("{folder_id:#018x}"),
                            jmap_mailbox_id = %mailbox.id,
                            folder_type = request.create_folder_type(),
                            open_existing = request.create_folder_open_existing(),
                            display_name = display_name,
                            message = "rca debug mapi create folder created real folder",
                        );
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties: HashMap::new(),
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            parent_folder_id,
                            Some(folder_id),
                        ));
                        output_handles.push(handle);
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::DeleteFolder) => {
                if request
                    .delete_folder_flags()
                    .is_none_or(|flags| flags & !0x15 != 0)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let Some(_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(folder_id) = request.delete_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let mailbox = folder_row_for_id(folder_id, mailboxes);
                if let Some(mailbox) = mailbox {
                    if mailbox.role != "custom" {
                        responses.extend_from_slice(&rop_error_response(
                            0x1D,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                } else if is_advertised_special_folder(folder_id) {
                    if !advertised_special_folder_delete_uses_session_tombstone(folder_id) {
                        if advertised_special_folder_delete_is_noop(folder_id) {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = principal.email.as_str(),
                                request_type = "Execute",
                                request_rop_id = "0x1d",
                                parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                                folder_id = %format!("{folder_id:#018x}"),
                                partial_completion = false,
                                message = "rca debug mapi delete advertised special folder no-op acknowledged",
                            );
                            responses.extend_from_slice(&rop_partial_completion_response(
                                0x1D,
                                request.response_handle_index(),
                                false,
                            ));
                            continue;
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = principal.email.as_str(),
                            request_type = "Execute",
                            request_rop_id = "0x1d",
                            parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                            folder_id = %format!("{folder_id:#018x}"),
                            response_error = "0x80070005",
                            message = "rca debug mapi delete advertised special folder denied",
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x1D,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                    session.record_deleted_advertised_special_folder(folder_id);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = principal.email.as_str(),
                        request_type = "Execute",
                        request_rop_id = "0x1d",
                        parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                        folder_id = %format!("{folder_id:#018x}"),
                        partial_completion = false,
                        message = "rca debug mapi delete advertised special folder acknowledged",
                    );
                    session.record_notification(MapiNotificationEvent::hierarchy(
                        _parent_folder_id,
                        Some(folder_id),
                    ));
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x1D,
                        request.response_handle_index(),
                        false,
                    ));
                    continue;
                }
                if let Some(public_folder) = snapshot.public_folder_for_id(folder_id) {
                    let partial_completion = store
                        .delete_public_folder(
                            principal.account_id,
                            public_folder.folder.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-delete-public-folder".to_string(),
                                subject: format!("public-folder:{}", public_folder.folder.id),
                            },
                        )
                        .await
                        .is_err();
                    if !partial_completion {
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            _parent_folder_id,
                            Some(folder_id),
                        ));
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x1D,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                let persisted_search_definition = snapshot
                    .search_folder_definition_for_folder_id(folder_id)
                    .cloned();
                let staged_search_definition = if persisted_search_definition.is_none() {
                    session.forget_search_folder_definition(folder_id)
                } else {
                    None
                };
                if let Some(definition) = persisted_search_definition
                    .as_ref()
                    .or(staged_search_definition.as_ref())
                {
                    if definition.is_builtin {
                        responses.extend_from_slice(&rop_error_response(
                            0x1D,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                    let partial_completion = if persisted_search_definition.is_some() {
                        store
                            .delete_search_folder(principal.account_id, definition.id)
                            .await
                            .is_err()
                    } else {
                        false
                    };
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        tenant_id = %principal.tenant_id,
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x1d",
                        parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                        folder_id = %format!("{folder_id:#018x}"),
                        search_folder_id = %definition.id,
                        display_name = %definition.display_name,
                        partial_completion = partial_completion,
                        message = "rca debug mapi delete search folder",
                    );
                    if !partial_completion {
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            _parent_folder_id,
                            Some(folder_id),
                        ));
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x1D,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                if session.search_folder_definition_was_deleted(folder_id) {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        tenant_id = %principal.tenant_id,
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x1d",
                        parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                        folder_id = %format!("{folder_id:#018x}"),
                        partial_completion = false,
                        message = "rca debug mapi delete search folder retry acknowledged",
                    );
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x1D,
                        request.response_handle_index(),
                        false,
                    ));
                    continue;
                }
                let Some(mailbox) = mailbox else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };

                let partial_completion = store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-folder".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                    .is_err();
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x1d",
                    parent_folder_id = %format!("{_parent_folder_id:#018x}"),
                    folder_id = %format!("{folder_id:#018x}"),
                    jmap_mailbox_id = %mailbox.id,
                    display_name = %mailbox.name,
                    role = %mailbox.role,
                    partial_completion = partial_completion,
                    message = "rca debug mapi delete real folder",
                );
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent::hierarchy(
                        _parent_folder_id,
                        Some(folder_id),
                    ));
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::MoveFolder | RopId::CopyFolder) => {
                let rop_id = request.rop_id;
                let response_handle_index = request.response_handle_index();
                if request.folder_move_copy_want_asynchronous().is_none()
                    || request.folder_move_copy_use_unicode().is_none()
                    || (rop_id == RopId::CopyFolder.as_u8()
                        && request.folder_move_copy_want_recursive().is_none())
                {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8007_0057,
                    ));
                    continue;
                }
                let Some(source_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(target_folder_id) = request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    })
                else {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(folder_id) = request.folder_move_copy_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8004_0102,
                    ));
                    continue;
                };
                let display_name = request.folder_move_copy_display_name();
                let display_name = display_name.trim();
                if display_name.is_empty() {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8004_0102,
                    ));
                    continue;
                }

                if rop_id == RopId::CopyFolder.as_u8() {
                    if let (Some(source_public_folder), Some(target_public_folder)) = (
                        snapshot.public_folder_for_id(folder_id),
                        snapshot.public_folder_for_id(target_folder_id),
                    ) {
                        let partial_completion = match copy_public_folder_tree_for_mapi(
                            store,
                            principal,
                            source_public_folder.folder.id,
                            target_public_folder.folder.id,
                            display_name,
                        )
                        .await
                        {
                            Ok(copied_folder) => {
                                if let Ok(copied_folder_id) = remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::PublicFolder,
                                    copied_folder.id,
                                    None,
                                    None,
                                )
                                .await
                                {
                                    session.record_notification(MapiNotificationEvent::hierarchy(
                                        target_folder_id,
                                        Some(copied_folder_id),
                                    ));
                                }
                                false
                            }
                            Err(_) => true,
                        };
                        responses.extend_from_slice(&rop_partial_completion_response(
                            rop_id,
                            response_handle_index,
                            partial_completion,
                        ));
                        continue;
                    }
                }
                if rop_id == RopId::MoveFolder.as_u8() {
                    if let (Some(source_public_folder), Some(target_public_folder)) = (
                        snapshot.public_folder_for_id(folder_id),
                        snapshot.public_folder_for_id(target_folder_id),
                    ) {
                        let source_parent_matches = snapshot
                            .public_folder_for_id(source_parent_folder_id)
                            .map(|parent| {
                                source_public_folder.folder.parent_folder_id
                                    == Some(parent.folder.id)
                            })
                            .unwrap_or(false);
                        if !source_parent_matches {
                            responses.extend_from_slice(&rop_error_response(
                                rop_id,
                                response_handle_index,
                                0x8004_010F,
                            ));
                            continue;
                        }
                        let partial_completion = store
                            .update_public_folder(
                                UpdatePublicFolderInput {
                                    account_id: principal.account_id,
                                    folder_id: source_public_folder.folder.id,
                                    parent_folder_id: Some(target_public_folder.folder.id),
                                    display_name: Some(display_name.to_string()),
                                    folder_class: None,
                                    sort_order: None,
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-public-folder".to_string(),
                                    subject: format!(
                                        "public-folder:{}->{}",
                                        source_public_folder.folder.id,
                                        target_public_folder.folder.id
                                    ),
                                },
                            )
                            .await
                            .is_err();
                        if !partial_completion {
                            session.record_notification(MapiNotificationEvent::hierarchy(
                                source_parent_folder_id,
                                Some(folder_id),
                            ));
                            session.record_notification(MapiNotificationEvent::hierarchy(
                                target_folder_id,
                                Some(folder_id),
                            ));
                        }
                        responses.extend_from_slice(&rop_partial_completion_response(
                            rop_id,
                            response_handle_index,
                            partial_completion,
                        ));
                        continue;
                    }
                }

                let target_parent_id = match target_folder_id {
                    IPM_SUBTREE_FOLDER_ID => None,
                    folder_id => match folder_row_for_id(folder_id, mailboxes) {
                        Some(mailbox) if mailbox.role == "custom" => Some(mailbox.id),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                rop_id,
                                response_handle_index,
                                0x8007_0005,
                            ));
                            continue;
                        }
                    },
                };
                let Some(source_mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8004_010F,
                    ));
                    continue;
                };
                if source_mailbox.role != "custom" {
                    responses.extend_from_slice(&rop_error_response(
                        rop_id,
                        response_handle_index,
                        0x8007_0005,
                    ));
                    continue;
                }

                let result = if request.rop_id == RopId::CopyFolder.as_u8() {
                    match store
                        .create_jmap_mailbox(
                            JmapMailboxCreateInput {
                                account_id: principal.account_id,
                                name: display_name.to_string(),
                                parent_id: target_parent_id,
                                sort_order: None,
                                is_subscribed: source_mailbox.is_subscribed,
                            },
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-copy-folder".to_string(),
                                subject: format!("folder:{}->{}", source_mailbox.id, display_name),
                            },
                        )
                        .await
                    {
                        Ok(mailbox) => {
                            match remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::Mailbox,
                                mailbox.id,
                                None,
                                None,
                            )
                            .await
                            {
                                Ok(copied_folder_id) => Ok((mailbox.id, copied_folder_id)),
                                Err(error) => Err(error),
                            }
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    store
                        .update_jmap_mailbox(
                            JmapMailboxUpdateInput {
                                account_id: principal.account_id,
                                mailbox_id: source_mailbox.id,
                                name: Some(display_name.to_string()),
                                parent_id: Some(target_parent_id),
                                sort_order: None,
                                is_subscribed: None,
                            },
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-move-folder".to_string(),
                                subject: format!("folder:{}", source_mailbox.id),
                            },
                        )
                        .await
                        .map(|mailbox| (mailbox.id, folder_id))
                };

                if let Ok((_changed_mailbox_id, changed_folder_id)) = result.as_ref() {
                    let old_parent_folder_id =
                        mailbox_parent_folder_id_for_dispatch(source_mailbox, mailboxes);
                    let new_parent_folder_id = target_parent_id
                        .and_then(|parent_id| {
                            mailboxes
                                .iter()
                                .find(|mailbox| mailbox.id == parent_id)
                                .map(mapi_folder_id)
                        })
                        .unwrap_or(IPM_SUBTREE_FOLDER_ID);
                    if request.rop_id == RopId::MoveFolder.as_u8() {
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            old_parent_folder_id,
                            Some(*changed_folder_id),
                        ));
                    }
                    session.record_notification(MapiNotificationEvent::hierarchy(
                        new_parent_folder_id,
                        Some(*changed_folder_id),
                    ));
                }
                let partial_completion = result.is_err();
                responses.extend_from_slice(&rop_partial_completion_response(
                    rop_id,
                    response_handle_index,
                    partial_completion,
                ));
            }
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages) => {
                if request.delete_messages_want_asynchronous().is_none()
                    || request.delete_messages_notify_non_read().is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ if request.rop_id == RopId::HardDeleteMessages.as_u8() => {
                        responses.extend_from_slice(&unsupported_rop_response(
                            request.rop_id,
                            request.response_handle_index(),
                        ));
                        continue;
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let mut partial_completion = false;
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_delete)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }
                if folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                for message_id in request.message_ids() {
                    if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
                        if request.rop_id == RopId::DeleteMessages.as_u8() {
                            partial_completion = true;
                            continue;
                        }
                        let Some(item) = snapshot.recoverable_item_for_id(folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        if store
                            .purge_recoverable_item(
                                principal.account_id,
                                item.canonical_id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-purge-recoverable-message".to_string(),
                                    subject: format!("recoverable:{}", item.canonical_id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_contact(principal.account_id, contact.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if !mapi_calendar_content_items_suppressed(folder_id, snapshot) {
                        if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                            if store
                                .delete_accessible_event(principal.account_id, event.canonical_id)
                                .await
                                .is_err()
                            {
                                partial_completion = true;
                            }
                            continue;
                        }
                    }
                    if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_task(principal.account_id, task.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        } else {
                            record_sync_upload_content_checkpoint(session, folder_id);
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        } else {
                            record_sync_upload_content_checkpoint(session, folder_id);
                        }
                        continue;
                    }
                    if let Some(message) = snapshot
                        .conversation_action_message_for_id(message_id)
                        .filter(|message| message.folder_id == folder_id)
                    {
                        if store
                            .delete_conversation_action(principal.account_id, message.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
                        if let Some(message) = snapshot
                            .navigation_shortcut_message_for_id(message_id)
                            .filter(|message| message.folder_id == folder_id)
                        {
                            if store
                                .delete_mapi_navigation_shortcut(
                                    principal.account_id,
                                    message.canonical_id,
                                )
                                .await
                                .is_err()
                            {
                                partial_completion = true;
                            }
                            continue;
                        }
                    }
                    if let Some(message) = snapshot
                        .associated_config_message_for_id(message_id)
                        .filter(|message| message.folder_id == folder_id)
                        .or_else(|| {
                            snapshot.associated_config_message_for_folder_and_source_key_id(
                                folder_id, message_id,
                            )
                        })
                    {
                        if store
                            .delete_mapi_associated_config(
                                principal.account_id,
                                message.canonical_id,
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        } else {
                            record_sync_upload_content_checkpoint(session, folder_id);
                        }
                        continue;
                    }
                    if folder_local_default_named_view_is_supported(snapshot, folder_id, message_id)
                    {
                        continue;
                    }
                    if let Some(item) = snapshot.public_folder_item_for_id(folder_id, message_id) {
                        if store
                            .delete_public_folder_item(
                                principal.account_id,
                                item.item.public_folder_id,
                                item.item.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-public-folder-item".to_string(),
                                    subject: format!("public-folder-item:{}", item.item.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.rop_id == 0x91
                        || email.mailbox_role == "trash"
                        || mailbox_is_trash_or_descendant(email.mailbox_id, mailboxes)
                    {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message-to-trash".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    } else {
                        record_sync_upload_content_checkpoint(session, folder_id);
                    }
                }
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent::content(folder_id, None));
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    request.rop_id,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::GetMessageStatus | RopId::SetMessageStatus) => {
                let response_rop_id = RopId::SetMessageStatus.as_u8();
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    Some(_) | None => {
                        responses.extend_from_slice(&rop_error_response(
                            response_rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let message_id = request.status_message_id().unwrap_or(0);
                let item_exists = message_for_id(folder_id, message_id, mailboxes, emails)
                    .or_else(|| {
                        emails
                            .iter()
                            .find(|email| mapi_item_id_matches(&email.id, message_id))
                    })
                    .is_some()
                    || snapshot
                        .public_folder_item_for_id(folder_id, message_id)
                        .is_some()
                    || snapshot.contact_for_id(folder_id, message_id).is_some()
                    || snapshot.event_for_id(folder_id, message_id).is_some()
                    || snapshot.task_for_id(folder_id, message_id).is_some();
                if !item_exists {
                    responses.extend_from_slice(&rop_error_response(
                        response_rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let key = (folder_id, message_id);
                let old_status = session.message_statuses.get(&key).copied().unwrap_or(0);
                if request.rop_id == 0x20 {
                    let mask = request.message_status_mask();
                    let new_status = (old_status & !mask) | (request.message_status_flags() & mask);
                    if new_status == 0 {
                        session.message_statuses.remove(&key);
                    } else {
                        session.message_statuses.insert(key, new_status);
                    }
                }
                responses.extend_from_slice(&rop_message_status_response(&request, old_status));
            }
            Some(RopId::FindRow) => {
                let find_trace = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::ContentsTable {
                        folder_id,
                        associated,
                        columns,
                        position,
                        restriction,
                        ..
                    }) if *folder_id == INBOX_FOLDER_ID => Some(format!(
                        "inbox_find_row:request_id={request_id};handle={};associated={associated};position={position};columns={};restriction={}",
                        format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        format_debug_property_tags(columns),
                        format_debug_restriction_option(restriction.as_ref())
                    )),
                    _ => None,
                };
                let selected_named_property_context = format_contents_table_named_property_context(
                    session,
                    input_object(session, &handle_slots, &request),
                );
                let response = find_row_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                    principal.account_id,
                );
                log_outlook_contents_table_find_row(
                    principal,
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    &selected_named_property_context,
                    snapshot,
                    &response,
                );
                if let Some(context) = format_inbox_associated_find_context(
                    input_object(session, &handle_slots, &request),
                    &request,
                    principal.account_id,
                    snapshot,
                    &response,
                ) {
                    session.record_last_inbox_associated_find_context(context);
                }
                if inbox_associated_broad_findrow_matched(
                    input_object(session, &handle_slots, &request),
                    &request,
                    &response,
                ) {
                    session.record_inbox_associated_broad_findrow(true);
                }
                if let Some(trace) = find_trace {
                    session.record_outlook_view_failure_trace_event(trace);
                }
                responses.extend_from_slice(&response);
            }
            Some(RopId::GetValidAttachments) => {
                responses.extend_from_slice(&rop_get_valid_attachments_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                    &session.pending_attachment_deletions,
                ))
            }
            Some(RopId::GetAttachmentTable) => {
                if !get_attachment_table_flags_are_valid(&request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x21,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0057,
                    ));
                    continue;
                }
                let (folder_id, message_id, is_calendar_event) =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::PendingMessage { folder_id, .. }) => {
                            (*folder_id, 0, false)
                        }
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                            ..
                        }) => (*folder_id, *message_id, false),
                        Some(MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        }) => (*folder_id, *message_id, true),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                0x21,
                                request.output_handle_index.unwrap_or(0),
                                0x8004_010F,
                            ));
                            continue;
                        }
                    };
                if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x21,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    attachment_table_object(folder_id, message_id),
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&get_attachment_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::OpenAttachment) => {
                if !open_attachment_flags_are_valid(&request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0057,
                    ));
                    continue;
                }
                let (folder_id, message_id, is_calendar_event) =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                            ..
                        }) => (*folder_id, *message_id, false),
                        Some(MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        }) => (*folder_id, *message_id, true),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                0x22,
                                request.output_handle_index.unwrap_or(0),
                                0x8004_010F,
                            ));
                            continue;
                        }
                    };
                if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                if session
                    .pending_attachment_deletions
                    .contains(&(folder_id, message_id, attach_num))
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                if snapshot
                    .attachment_for_message(folder_id, message_id, attach_num)
                    .is_some()
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Attachment {
                            folder_id,
                            message_id,
                            attach_num,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_attachment_response(&request));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::CreateAttachment) => {
                let parent_message_handle =
                    input_handle(&handle_slots, &request).filter(|handle| {
                        matches!(
                            session.handles.get(handle),
                            Some(MapiObject::PendingMessage { .. })
                        )
                    });
                let (folder_id, message_id, is_calendar_event, is_pending_message) =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::Event { folder_id, .. })
                            if mapi_calendar_content_items_suppressed(*folder_id, snapshot) =>
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x23,
                                request.output_handle_index.unwrap_or(0),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                            ..
                        }) => (*folder_id, *message_id, false, false),
                        Some(MapiObject::PendingMessage { folder_id, .. }) => {
                            (*folder_id, 0, false, true)
                        }
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => (*folder_id, *event_id, true, false),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                0x23,
                                request.output_handle_index.unwrap_or(0),
                                0x0000_04B9,
                            ));
                            continue;
                        }
                    };
                if !is_calendar_event
                    && !is_pending_message
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }

                let attach_num = if let Some(parent_handle) = parent_message_handle {
                    session
                        .pending_message_attachments
                        .get(&parent_handle)
                        .and_then(|attachments| {
                            attachments.iter().map(|(attach_num, _)| *attach_num).max()
                        })
                        .unwrap_or(u32::MAX)
                        .saturating_add(1)
                } else {
                    next_pending_attachment_num(session, folder_id, message_id, snapshot)
                };
                let created_at = current_mapi_filetime();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingAttachment {
                        folder_id,
                        message_id,
                        attach_num,
                        properties: HashMap::from([
                            (PID_TAG_ATTACH_SIZE, MapiValue::U32(0)),
                            (PID_TAG_ACCESS_LEVEL, MapiValue::U32(0)),
                            (PID_TAG_CREATION_TIME, MapiValue::U64(created_at)),
                            (PID_TAG_LAST_MODIFICATION_TIME, MapiValue::U64(created_at)),
                        ]),
                        data: Vec::new(),
                    },
                );
                if let Some(parent_handle) = parent_message_handle {
                    session
                        .pending_attachment_parent_messages
                        .insert(handle, parent_handle);
                }
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_attachment_response(&request, attach_num));
                output_handles.push(handle);
            }
            Some(RopId::DeleteAttachment) => {
                let (folder_id, message_id, is_calendar_event) =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::Event { folder_id, .. })
                            if mapi_calendar_content_items_suppressed(*folder_id, snapshot) =>
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x24,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                            ..
                        }) => (*folder_id, *message_id, false),
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => (*folder_id, *event_id, true),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                0x24,
                                request.response_handle_index(),
                                0x0000_04B9,
                            ));
                            continue;
                        }
                    };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                let Some(attachment) =
                    snapshot.attachment_for_message(folder_id, message_id, attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }
                let _ = is_calendar_event;
                let _ = attachment;
                session
                    .pending_attachment_deletions
                    .insert((folder_id, message_id, attach_num));
                responses.extend_from_slice(&rop_simple_success_response(&request));
            }
            Some(RopId::OpenEmbeddedMessage) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x46,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let open_mode = request.payload.get(2).copied().unwrap_or(0);
                if open_mode > 0x02 {
                    responses.extend_from_slice(&rop_error_response(
                        0x46,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let Some((folder_id, message_id, attach_num, embedded_properties)) =
                    open_embedded_message_source(
                        store, principal, session, snapshot, handle, open_mode,
                    )
                    .await
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x46,
                        request.response_handle_index(),
                        if open_mode == 0 {
                            0x8004_010F
                        } else {
                            0x8007_0005
                        },
                    ));
                    continue;
                };
                let embedded_message_id =
                    transient_embedded_message_id(folder_id, message_id, attach_num);
                let embedded_subject = embedded_message_open_subject(&embedded_properties);
                let embedded_handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingMessage {
                        folder_id,
                        properties: embedded_properties,
                        recipients: Vec::new(),
                    },
                );
                session
                    .pending_embedded_message_ids
                    .insert(embedded_handle, embedded_message_id);
                session
                    .pending_embedded_message_attachments
                    .insert(embedded_handle, (folder_id, message_id, attach_num));
                set_handle_slot(
                    &mut handle_slots,
                    request.output_handle_index,
                    embedded_handle,
                );
                responses.extend_from_slice(&rop_open_embedded_message_response(
                    &request,
                    embedded_message_id,
                    &embedded_subject,
                    0,
                ));
                output_handles.push(embedded_handle);
            }
            Some(RopId::SaveChangesAttachment) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if !save_flags_are_supported(&request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let save_attachment_object = session.handles.get(&handle).cloned();
                session.record_recent_probe_action(format!(
                    "SaveChangesAttachment(in={},handle={},kind={},folder={})",
                    request.input_handle_index().unwrap_or(0),
                    handle,
                    mapi_object_debug_kind(save_attachment_object.as_ref()),
                    mapi_object_debug_folder_id(save_attachment_object.as_ref())
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x25",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = handle,
                    object_kind = mapi_object_debug_kind(save_attachment_object.as_ref()),
                    folder_id = %mapi_object_debug_folder_id(save_attachment_object.as_ref()),
                    "rca debug mapi save changes before inbox probe"
                );
                let Some(MapiObject::PendingAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    properties,
                    data,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }
                let mut attachment = pending_attachment_upload(attach_num, &properties, data);
                let attach_method = properties
                    .get(&PID_TAG_ATTACH_METHOD)
                    .and_then(MapiValue::as_i64)
                    .unwrap_or(1);
                let mut generated_embedded_attachment = false;
                if attach_method == 5 {
                    if let Some(embedded_properties) = session
                        .saved_embedded_messages
                        .get(&(folder_id, message_id, attach_num))
                    {
                        attachment = pending_embedded_message_attachment_upload(
                            attach_num,
                            &properties,
                            embedded_properties,
                        );
                        generated_embedded_attachment = true;
                    }
                }
                let mut attachment = attachment;
                if !generated_embedded_attachment {
                    let validation = validator.validate_bytes(
                        ValidationRequest {
                            ingress_context: IngressContext::ExchangeAttachment,
                            declared_mime: Some(attachment.media_type.clone()),
                            filename: Some(attachment.file_name.clone()),
                            expected_kind: mapi_expected_attachment_kind(
                                &attachment.media_type,
                                &attachment.file_name,
                            ),
                        },
                        &attachment.blob_bytes,
                    );
                    let Ok(outcome) = validation else {
                        responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    };
                    if outcome.policy_decision != PolicyDecision::Accept {
                        responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    if attachment.media_type == "application/octet-stream"
                        && !outcome.detected_mime.trim().is_empty()
                    {
                        attachment.media_type = outcome.detected_mime;
                    }
                }
                if let Some(parent_handle) = session
                    .pending_attachment_parent_messages
                    .get(&handle)
                    .copied()
                {
                    session
                        .pending_message_attachments
                        .entry(parent_handle)
                        .or_default()
                        .retain(|(existing_attach_num, _)| *existing_attach_num != attach_num);
                    session
                        .pending_message_attachments
                        .entry(parent_handle)
                        .or_default()
                        .push((attach_num, attachment.clone()));
                    session.handles.insert(
                        handle,
                        MapiObject::SavedAttachment {
                            folder_id,
                            message_id,
                            attach_num,
                            file_reference: format!("pending-message:{parent_handle}:{attach_num}"),
                            file_name: attachment.file_name,
                            media_type: attachment.media_type,
                            disposition: attachment.disposition,
                            content_id: attachment.content_id,
                            size_octets: attachment.blob_bytes.len() as u64,
                        },
                    );
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                    continue;
                }
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    match store
                        .add_message_attachment(
                            principal.account_id,
                            email.id,
                            attachment,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-save-attachment".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                    {
                        Ok(Some((_email, stored))) => {
                            if upsert_custom_property_values_from_map(
                                store,
                                principal,
                                MapiCustomPropertyObjectKind::Attachment,
                                stored.id,
                                &properties,
                            )
                            .await
                            .is_err()
                            {
                                responses.extend_from_slice(&rop_error_response(
                                    0x25,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                            session.handles.insert(
                                handle,
                                MapiObject::SavedAttachment {
                                    folder_id,
                                    message_id,
                                    attach_num,
                                    file_reference: stored.file_reference,
                                    file_name: stored.file_name,
                                    media_type: stored.media_type,
                                    disposition: stored.disposition,
                                    content_id: stored.content_id,
                                    size_octets: stored.size_octets,
                                },
                            );
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                        _ => responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                } else if !mapi_calendar_content_items_suppressed(folder_id, snapshot) {
                    if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                        match store
                            .add_calendar_event_attachment(
                                principal.account_id,
                                event.canonical_id,
                                attachment,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-save-calendar-attachment".to_string(),
                                    subject: format!("calendar-event:{}", event.canonical_id),
                                },
                            )
                            .await
                        {
                            Ok(Some(stored)) => {
                                if upsert_custom_property_values_from_map(
                                    store,
                                    principal,
                                    MapiCustomPropertyObjectKind::Attachment,
                                    stored.id,
                                    &properties,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x25,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::SavedAttachment {
                                        folder_id,
                                        message_id,
                                        attach_num,
                                        file_reference: stored.file_reference,
                                        file_name: stored.file_name,
                                        media_type: stored.media_type,
                                        disposition: None,
                                        content_id: None,
                                        size_octets: stored.size_octets,
                                    },
                                );
                                responses.extend_from_slice(&rop_simple_success_response(&request));
                            }
                            _ => responses.extend_from_slice(&rop_error_response(
                                0x25,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::OpenStream) => {
                let Some(input_handle) = input_handle(&handle_slots, &request) else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x2b",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = "missing",
                        response_handle_index = request.response_handle_index(),
                        output_handle_index = request.output_handle_index.unwrap_or(0),
                        stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
                        stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
                        stream_open_result = "missing_input_handle",
                        message = "rca debug mapi open stream"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let input_object_kind = mapi_object_debug_kind(session.handles.get(&input_handle));
                let input_folder_id =
                    mapi_object_debug_folder_id(session.handles.get(&input_handle));
                let (associated_config_id, associated_config_class, associated_config_subject) =
                    associated_config_debug_fields(session, snapshot, input_handle);
                let is_inbox_associated_config_stream = matches!(
                    session.handles.get(&input_handle),
                    Some(MapiObject::AssociatedConfig {
                        folder_id: INBOX_FOLDER_ID,
                        ..
                    })
                );
                let is_inbox_rule_organizer_stream = is_inbox_associated_config_stream
                    && associated_config_class
                        == crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS
                    && request.stream_property_tag().unwrap_or(0)
                        == OUTLOOK_RULE_ORGANIZER_BINARY_6802;
                if is_inbox_associated_config_stream {
                    session.record_inbox_associated_config_stream_open();
                    session.record_outlook_view_failure_trace_event(format!(
                        "open_inbox_config_stream:request_id={request_id};input_handle={input_handle};tag=0x{:08x};mode=0x{:02x};class={associated_config_class};subject={associated_config_subject}",
                        request.stream_property_tag().unwrap_or(0),
                        request.stream_open_mode().unwrap_or(0)
                    ));
                    session.record_recent_probe_action(format!(
                        "OpenAssociatedConfigStream(in={},tag=0x{:08x},mode=0x{:02x})",
                        input_handle,
                        request.stream_property_tag().unwrap_or(0),
                        request.stream_open_mode().unwrap_or(0)
                    ));
                }
                let Some((stream_data, writable_target)) = open_stream_data(
                    store,
                    principal,
                    session,
                    input_handle,
                    request.stream_property_tag().unwrap_or(0),
                    request.stream_open_mode().unwrap_or(0),
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await
                else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x2b",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = input_handle,
                        response_handle_index = request.response_handle_index(),
                        output_handle_index = request.output_handle_index.unwrap_or(0),
                        object_kind = input_object_kind,
                        folder_id = %input_folder_id,
                        associated_config_id = %associated_config_id,
                        associated_config_class = %associated_config_class,
                        associated_config_subject = %associated_config_subject,
                        stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
                        stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
                        stream_open_result = "missing_stream_data",
                        inbox_associated_config_stream = is_inbox_associated_config_stream,
                        message = "rca debug mapi open stream"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = stream_data.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: stream_data,
                        position: 0,
                        writable_target,
                    },
                );
                if is_inbox_associated_config_stream {
                    session.record_inbox_associated_config_stream_handle(handle);
                    session.record_outlook_view_failure_trace_event(format!(
                        "open_inbox_config_stream_result:request_id={request_id};input_handle={input_handle};output_handle={handle};size={stream_size};writable={}",
                        writable_target.is_some()
                    ));
                }
                if is_inbox_rule_organizer_stream {
                    session.record_inbox_rule_organizer_stream_handle(handle);
                    session.record_recent_probe_action(format!(
                        "OpenRuleOrganizerStream(in={},out={},size={})",
                        input_handle, handle, stream_size
                    ));
                }
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x2b",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = input_handle,
                    response_handle_index = request.response_handle_index(),
                    output_handle_index = request.output_handle_index.unwrap_or(0),
                    output_handle_value = handle,
                    object_kind = input_object_kind,
                    folder_id = %input_folder_id,
                    associated_config_id = %associated_config_id,
                    associated_config_class = %associated_config_class,
                    associated_config_subject = %associated_config_subject,
                    stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
                    stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
                    stream_size,
                    stream_empty = stream_size == 0,
                    stream_preview = %hex_preview(
                        match session.handles.get(&handle) {
                            Some(MapiObject::AttachmentStream { data, .. }) => data.as_slice(),
                            _ => &[],
                        },
                        32
                    ),
                    stream_open_result = "success",
                    inbox_associated_config_stream = is_inbox_associated_config_stream,
                    inbox_rule_organizer_stream = is_inbox_rule_organizer_stream,
                    message = "rca debug mapi open stream"
                );
                responses.extend_from_slice(&rop_open_stream_response(&request, stream_size));
                output_handles.push(handle);
            }
            Some(RopId::ReadStream) => {
                let read_input_handle = input_handle(&handle_slots, &request);
                let resolved_stream_handle = read_input_handle
                    .and_then(|handle| resolve_writable_stream_handle(session, handle));
                let is_rule_organizer_stream_read = resolved_stream_handle
                    .is_some_and(|handle| session.is_inbox_rule_organizer_stream_handle(handle));
                if let Some(stream_handle) = resolved_stream_handle {
                    if session.is_inbox_associated_config_stream_handle(stream_handle) {
                        session.record_inbox_associated_config_stream_read();
                        session.record_outlook_view_failure_trace_event(format!(
                            "read_inbox_config_stream:request_id={request_id};handle={stream_handle};requested_bytes={}",
                            request.read_byte_count().unwrap_or(0)
                        ));
                        session.record_recent_probe_action(format!(
                            "ReadAssociatedConfigStream(in={},max={})",
                            stream_handle,
                            request.read_byte_count().unwrap_or(0)
                        ));
                    }
                }
                let Some(stream) =
                    resolved_stream_handle.and_then(|handle| session.handles.get_mut(&handle))
                else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x2c",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = read_input_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "missing".to_string()),
                        resolved_stream_handle = resolved_stream_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        response_handle_index = request.response_handle_index(),
                        requested_byte_count = request.read_byte_count().unwrap_or(0),
                        stream_read_result = "missing_input_object",
                        message = "rca debug mapi read stream"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x2C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (before_position, stream_len) = match stream {
                    MapiObject::AttachmentStream { data, position, .. } => (*position, data.len()),
                    _ => (0, 0),
                };
                let response = rop_read_stream_response(&request, stream);
                let after_position = match stream {
                    MapiObject::AttachmentStream { position, .. } => *position,
                    _ => 0,
                };
                let returned_byte_count = after_position.saturating_sub(before_position);
                let end_of_stream = after_position >= stream_len;
                if is_rule_organizer_stream_read {
                    let context = format!(
                        "input_handle={};requested_byte_count={};stream_size={};position_before={};position_after={};returned_byte_count={};end_of_stream={};response_bytes={};response_preview={}",
                        read_input_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "missing".to_string()),
                        request.read_byte_count().unwrap_or(0),
                        stream_len,
                        before_position,
                        after_position,
                        returned_byte_count,
                        end_of_stream,
                        response.len(),
                        hex_preview(&response, 48)
                    );
                    session.record_inbox_rule_organizer_stream_read(context.clone());
                    session.record_recent_probe_action(format!(
                        "ReadRuleOrganizerStream(in={},returned={},eos={})",
                        read_input_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "missing".to_string()),
                        returned_byte_count,
                        end_of_stream
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x2c",
                        rule_organizer_stream_context = %context,
                        inbox_loop_summary =
                            %format_inbox_open_loop_summary(
                                &session.post_hierarchy_actions
                            )
                            .unwrap_or_else(|| "none".to_string()),
                        "rca debug outlook rule organizer stream read checkpoint"
                    );
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x2c",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = read_input_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "missing".to_string()),
                    resolved_stream_handle = resolved_stream_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    response_handle_index = request.response_handle_index(),
                    requested_byte_count = request.read_byte_count().unwrap_or(0),
                    stream_position_before = before_position,
                    stream_position_after = after_position,
                    returned_byte_count,
                    end_of_stream,
                    response_bytes = response.len(),
                    response_preview = %hex_preview(&response, 48),
                    stream_read_result = "success",
                    inbox_rule_organizer_stream = is_rule_organizer_stream_read,
                    message = "rca debug mapi read stream"
                );
                responses.extend_from_slice(&response);
            }
            Some(RopId::SeekStream) => {
                let requested_handle = input_handle(&handle_slots, &request);
                let stream_handle = requested_handle
                    .and_then(|handle| resolve_writable_stream_handle(session, handle));
                let Some(stream) =
                    stream_handle.and_then(|handle| session.handles.get_mut(&handle))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x2e",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    requested_handle = requested_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "missing".to_string()),
                    resolved_stream_handle = stream_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    message = "rca debug mapi seek stream"
                );
                responses.extend_from_slice(&rop_seek_stream_response(&request, stream));
            }
            Some(RopId::SetStreamSize) => {
                let Some(requested_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_handle = resolve_writable_stream_handle(session, requested_handle);
                if stream_handle
                    .is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
                {
                    session.record_outlook_view_failure_trace_event(format!(
                        "set_inbox_config_stream_size:request_id={request_id};requested_handle={requested_handle};resolved_handle={};size={}",
                        stream_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        request.stream_size().unwrap_or(u64::MAX)
                    ));
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x2f",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    requested_handle,
                    resolved_stream_handle = stream_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    requested_stream_size = request.stream_size().unwrap_or(u64::MAX),
                    requested_object_kind = mapi_object_debug_kind(session.handles.get(&requested_handle)),
                    resolved_object_kind = stream_handle
                        .and_then(|handle| session.handles.get(&handle))
                        .map(|object| mapi_object_debug_kind(Some(object)))
                        .unwrap_or("none"),
                    message = "rca debug mapi set stream size"
                );
                match set_attachment_stream_size(
                    session,
                    stream_handle.unwrap_or(requested_handle),
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some(()) => responses.extend_from_slice(&rop_simple_success_response(&request)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::WriteStream | RopId::WriteAndCommitStream | RopId::WriteStreamExtended) => {
                let Some(requested_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_handle = resolve_writable_stream_handle(session, requested_handle);
                if stream_handle
                    .is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
                {
                    session.record_outlook_view_failure_trace_event(format!(
                        "write_inbox_config_stream:request_id={request_id};requested_handle={requested_handle};resolved_handle={};bytes={}",
                        stream_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        request.stream_write_data().len()
                    ));
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = %format!("0x{:02x}", request.rop_id),
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    requested_handle,
                    resolved_stream_handle = stream_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    write_byte_count = request.stream_write_data().len(),
                    requested_object_kind = mapi_object_debug_kind(session.handles.get(&requested_handle)),
                    resolved_object_kind = stream_handle
                        .and_then(|handle| session.handles.get(&handle))
                        .map(|object| mapi_object_debug_kind(Some(object)))
                        .unwrap_or("none"),
                    message = "rca debug mapi write stream"
                );
                let stream_handle = stream_handle.unwrap_or(requested_handle);
                match write_stream(session, stream_handle, request.stream_write_data()) {
                    Some(written) => {
                        responses.extend_from_slice(&rop_write_stream_response(&request, written))
                    }
                    None => {
                        let error_code = stream_write_error_code(
                            stream_write_error(session, stream_handle)
                                .unwrap_or(StreamWriteError::NotFound),
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            error_code,
                        ))
                    }
                }
            }
            Some(RopId::CopyToStream) => {
                let Some(source_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let source_handle =
                    resolve_writable_stream_handle(session, source_handle).unwrap_or(source_handle);
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let destination_handle =
                    resolve_writable_stream_handle(session, destination_handle)
                        .unwrap_or(destination_handle);
                match copy_stream(
                    session,
                    source_handle,
                    destination_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some((read, written)) => responses
                        .extend_from_slice(&rop_copy_to_stream_response(&request, read, written)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::CopyTo) => {
                if !matches!(request.copy_to_want_asynchronous(), Some(0x00 | 0x01))
                    || !matches!(request.copy_to_want_subobjects(), Some(0x00 | 0x01))
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x39,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_copy_to_null_destination_response(&request));
                    continue;
                };
                let destination_object = session.handles.get(&destination_handle).cloned();
                if destination_object.is_none() {
                    responses.extend_from_slice(&rop_copy_to_null_destination_response(&request));
                    continue;
                }
                let source_object = input_object(session, &handle_slots, &request).cloned();
                if source_object.is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x39,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if copy_all_custom_property_values_for_request(
                    store,
                    principal,
                    source_object.as_ref(),
                    destination_object.as_ref(),
                    mailboxes,
                    emails,
                    snapshot,
                    &request.copy_to_excluded_property_tags(),
                )
                .await
                .unwrap_or(false)
                {
                    responses.extend_from_slice(&rop_set_properties_response(&request));
                    continue;
                }
                if copy_all_message_followup_property_values_for_request(
                    store,
                    principal,
                    source_object.as_ref(),
                    destination_object.as_ref(),
                    mailboxes,
                    emails,
                    snapshot,
                    &request.copy_to_excluded_property_tags(),
                )
                .await
                .unwrap_or(false)
                {
                    responses.extend_from_slice(&rop_set_properties_response(&request));
                    continue;
                }
                responses.extend_from_slice(&unsupported_rop_response(
                    0x39,
                    request.response_handle_index(),
                ));
            }
            Some(RopId::CopyProperties) => {
                if !matches!(
                    request.copy_properties_want_asynchronous(),
                    Some(0x00 | 0x01)
                ) {
                    responses.extend_from_slice(&rop_error_response(
                        0x67,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x67,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_copy_properties_null_destination_response(
                        &request,
                    ));
                    continue;
                };
                if !session.handles.contains_key(&destination_handle) {
                    responses.extend_from_slice(&rop_copy_properties_null_destination_response(
                        &request,
                    ));
                    continue;
                }
                if request.copy_properties_property_tags().is_empty() {
                    responses.extend_from_slice(&rop_copy_properties_success_response(&request));
                    continue;
                }
                let source_object = input_object(session, &handle_slots, &request).cloned();
                let destination_object = session.handles.get(&destination_handle).cloned();
                if let Some(problems) = copy_message_followup_property_values_for_request(
                    store,
                    principal,
                    source_object.as_ref(),
                    destination_object.as_ref(),
                    mailboxes,
                    emails,
                    snapshot,
                    &request.copy_properties_property_tags(),
                )
                .await
                .unwrap_or_default()
                {
                    if problems.is_empty() {
                        responses
                            .extend_from_slice(&rop_copy_properties_success_response(&request));
                    } else {
                        responses.extend_from_slice(&rop_set_properties_problem_response(
                            &request, &problems,
                        ));
                    }
                    continue;
                }
                if let Some(problems) = copy_custom_property_values_for_request(
                    store,
                    principal,
                    source_object.as_ref(),
                    destination_object.as_ref(),
                    mailboxes,
                    emails,
                    snapshot,
                    &request.copy_properties_property_tags(),
                )
                .await
                .unwrap_or_default()
                {
                    if problems.is_empty() {
                        responses
                            .extend_from_slice(&rop_copy_properties_success_response(&request));
                    } else {
                        responses.extend_from_slice(&rop_set_properties_problem_response(
                            &request, &problems,
                        ));
                    }
                    continue;
                }
                responses.extend_from_slice(&unsupported_rop_response(
                    0x67,
                    request.response_handle_index(),
                ));
            }
            Some(RopId::GetStreamSize) => {
                let requested_handle = input_handle(&handle_slots, &request);
                let stream_handle = requested_handle
                    .and_then(|handle| resolve_writable_stream_handle(session, handle));
                match stream_handle.and_then(|handle| session.handles.get(&handle)) {
                    Some(MapiObject::AttachmentStream { data, .. }) => responses
                        .extend_from_slice(&rop_get_stream_size_response(&request, data.len())),
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x5E,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::CloneStream) => {
                let requested_handle = input_handle(&handle_slots, &request);
                let stream_handle = requested_handle
                    .and_then(|handle| resolve_writable_stream_handle(session, handle));
                match stream_handle
                    .and_then(|handle| session.handles.get(&handle))
                    .cloned()
                {
                    Some(MapiObject::AttachmentStream {
                        data,
                        position,
                        writable_target: None,
                    }) => {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::AttachmentStream {
                                data,
                                position,
                                writable_target: None,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                        output_handles.push(handle);
                    }
                    Some(MapiObject::AttachmentStream { .. }) => responses.extend_from_slice(
                        &rop_error_response(0x3B, request.response_handle_index(), 0x8004_0102),
                    ),
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x3B,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::LockRegionStream | RopId::UnlockRegionStream) => {
                let requested_handle = input_handle(&handle_slots, &request);
                match requested_handle.and_then(|handle| session.handles.get(&handle)) {
                    Some(MapiObject::AttachmentStream { .. }) => {
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SetSpooler) => {
                responses.extend_from_slice(&spooler_advisory_response(
                    &request,
                    input_handle(&handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::SpoolerLockMessage | RopId::TransportNewMail) => {
                responses.extend_from_slice(&spooler_advisory_response(
                    &request,
                    input_handle(&handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::UpdateDeferredActionMessages) => {
                responses.extend_from_slice(&deferred_action_messages_response(
                    &request,
                    input_handle(&handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::CommitStream) => {
                let requested_handle = input_handle(&handle_slots, &request);
                let stream_handle = requested_handle
                    .and_then(|handle| resolve_writable_stream_handle(session, handle));
                if stream_handle
                    .is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
                {
                    session.record_outlook_view_failure_trace_event(format!(
                        "commit_inbox_config_stream:request_id={request_id};requested_handle={};resolved_handle={}",
                        requested_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "missing".to_string()),
                        stream_handle
                            .map(|handle| handle.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    ));
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x5d",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    requested_handle = requested_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "missing".to_string()),
                    resolved_stream_handle = stream_handle
                        .map(|handle| handle.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    message = "rca debug mapi commit stream"
                );
                let commit_object = stream_handle
                    .and_then(|handle| session.handles.get(&handle))
                    .or_else(|| input_object(session, &handle_slots, &request))
                    .cloned();
                let commit_result = match commit_object {
                    Some(MapiObject::AttachmentStream {
                        writable_target:
                            Some(StreamWriteTarget::AssociatedConfigProperty { handle, .. }),
                        ..
                    }) => {
                        let message = match session.handles.get(&handle) {
                            Some(MapiObject::AssociatedConfig {
                                folder_id,
                                saved_message: Some(message),
                                ..
                            }) => Some((*folder_id, message.clone())),
                            _ => None,
                        };
                        match message {
                            Some((folder_id, message)) => {
                                persist_associated_config_stream_message(
                                    store, principal, folder_id, &message,
                                )
                                .await
                            }
                            None => Err(anyhow!(
                                "MAPI associated config stream commit target was not found"
                            )),
                        }
                    }
                    Some(MapiObject::AttachmentStream { .. }) => Ok(()),
                    _ => Err(anyhow!("MAPI stream commit target was not found")),
                };
                match commit_result {
                    Ok(()) => responses.extend_from_slice(&rop_simple_success_response(&request)),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x5D,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SubmitMessage | RopId::TransportSend) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = %format!("{:#04x}", request.rop_id),
                        response_handle_index = request.response_handle_index(),
                        failure_reason = "missing_input_handle",
                        "rca debug mapi submit message"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(object) = session.handles.get(&handle).cloned() else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = %format!("{:#04x}", request.rop_id),
                        input_handle = handle,
                        response_handle_index = request.response_handle_index(),
                        failure_reason = "session_handle_not_found",
                        "rca debug mapi submit message"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let input = match object {
                    MapiObject::PendingMessage {
                        properties,
                        recipients,
                        ..
                    } => mapi_submit_from_pending_message(principal, &properties, &recipients),
                    MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email,
                        ..
                    } => {
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                            .or(saved_email.as_ref().map(|saved| &saved.email))
                        else {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = %format!("{:#04x}", request.rop_id),
                                input_handle = handle,
                                object_kind = "message",
                                folder_id = %format!("{folder_id:#018x}"),
                                message_id = %format!("{message_id:#018x}"),
                                failure_reason = "message_identity_not_found",
                                "rca debug mapi submit message"
                            );
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        if !submit_source_is_outgoing(email) {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = %format!("{:#04x}", request.rop_id),
                                input_handle = handle,
                                object_kind = "message",
                                folder_id = %format!("{folder_id:#018x}"),
                                message_id = %format!("{message_id:#018x}"),
                                mailbox_role = %email.mailbox_role,
                                failure_reason = "message_not_in_outgoing_folder",
                                "rca debug mapi submit message"
                            );
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        match mapi_submit_from_existing_email(store, principal, email).await {
                            Ok(input) => input,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to build canonical input for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        }
                    }
                    _ => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = %format!("{:#04x}", request.rop_id),
                            input_handle = handle,
                            failure_reason = "unsupported_object_for_submit",
                            "rca debug mapi submit message"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = %format!("{:#04x}", request.rop_id),
                    input_handle = handle,
                    subject = %input.subject,
                    to_count = input.to.len(),
                    cc_count = input.cc.len(),
                    bcc_count = input.bcc.len(),
                    attachment_count = input.attachments.len(),
                    body_text_bytes = input.body_text.len(),
                    body_html_bytes = input
                        .body_html_sanitized
                        .as_deref()
                        .map(str::len)
                        .unwrap_or(0),
                    draft_message_id = %input.draft_message_id.map(|id| id.to_string()).unwrap_or_default(),
                    source = %input.source,
                    "rca debug mapi submit message"
                );
                match store
                    .submit_message(input, submit_audit_entry(principal, handle))
                    .await
                {
                    Ok(submitted) => {
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            submitted.message_id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            submitted_message_handle_object(&submitted, mailboxes, message_id),
                        );
                        match store
                            .fetch_jmap_emails(principal.account_id, &[submitted.message_id])
                            .await
                        {
                            Ok(mut emails) => created_emails.append(&mut emails),
                            Err(error) => tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = %format!("{:#04x}", request.rop_id),
                                input_handle = handle,
                                submitted_message_id = %submitted.message_id,
                                load_error = %error,
                                failure_reason = "submitted_message_same_execute_load_failed",
                                "rca debug mapi submit message"
                            ),
                        }
                        responses.extend_from_slice(&submit_success_response(&request));
                    }
                    Err(error) => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = %format!("{:#04x}", request.rop_id),
                            input_handle = handle,
                            submit_error = %error,
                            failure_reason = "canonical_submit_failed",
                            "rca debug mapi submit message"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                }
            }
            Some(RopId::AbortSubmit) => {
                let Some(folder_id) = request.abort_submit_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x34,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                let Some(message_id) = request.abort_submit_message_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x34,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                let canonical_message_id = abort_submit_canonical_message_id(
                    store,
                    principal.account_id,
                    folder_id,
                    message_id,
                    mailboxes,
                    emails,
                )
                .await;
                if canonical_message_id.is_none()
                    && message_for_id(folder_id, message_id, mailboxes, emails)
                        .is_some_and(|email| !abort_submit_source_is_sent(email))
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x34,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(canonical_message_id) = canonical_message_id else {
                    responses.extend_from_slice(&rop_error_response(
                        0x34,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let cancel_result = store
                    .cancel_queued_submission(
                        principal.account_id,
                        canonical_message_id,
                        abort_submit_audit_entry(principal, canonical_message_id),
                    )
                    .await;
                responses.extend_from_slice(&abort_submit_cancel_response(&request, cancel_result));
            }
            Some(RopId::MoveCopyMessages) => {
                if request.move_copy_want_asynchronous().is_none()
                    || request.move_copy_want_copy_raw().is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let source_folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        tracing::info!(
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x33",
                            input_handle_index = request.input_handle_index().unwrap_or(0),
                            message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                            want_copy = request.move_copy_want_copy(),
                            failure = "source_handle_not_folder",
                            "rca debug mapi move copy messages failure"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let target_folder_id = match request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    }) {
                    Some(folder_id) => folder_id,
                    None => {
                        tracing::info!(
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x33",
                            source_folder_id = format_args!("0x{source_folder_id:016x}"),
                            message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                            want_copy = request.move_copy_want_copy(),
                            failure = "target_handle_not_folder",
                            "rca debug mapi move copy messages failure"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                if source_folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
                    tracing::info!(
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x33",
                        source_folder_id = format_args!("0x{source_folder_id:016x}"),
                        target_folder_id = format_args!("0x{target_folder_id:016x}"),
                        message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                        want_copy = request.move_copy_want_copy(),
                        failure = "recoverable_items_root_source",
                        "rca debug mapi move copy messages failure"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if matches!(source_folder_id, NOTES_FOLDER_ID | JOURNAL_FOLDER_ID) {
                    let mut partial_completion = false;
                    for message_id in request.move_copy_message_ids() {
                        if source_folder_id == NOTES_FOLDER_ID {
                            let Some(note) = snapshot.note_for_id(source_folder_id, message_id)
                            else {
                                partial_completion = true;
                                continue;
                            };
                            if target_folder_id != NOTES_FOLDER_ID {
                                partial_completion = true;
                                continue;
                            }
                            if request.move_copy_want_copy() {
                                match store
                                    .upsert_mapi_note(UpsertClientNoteInput {
                                        id: None,
                                        account_id: principal.account_id,
                                        title: note.note.title.clone(),
                                        body_text: note.note.body_text.clone(),
                                        color: note.note.color.clone(),
                                        categories_json: note.note.categories_json.clone(),
                                    })
                                    .await
                                {
                                    Ok(copied) => {
                                        if remember_created_mapi_identity(
                                            store,
                                            principal,
                                            MapiIdentityObjectKind::Note,
                                            copied.id,
                                            None,
                                            None,
                                        )
                                        .await
                                        .is_err()
                                        {
                                            partial_completion = true;
                                        }
                                    }
                                    Err(_) => partial_completion = true,
                                }
                            }
                            continue;
                        }
                        let Some(entry) =
                            snapshot.journal_entry_for_id(source_folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        if target_folder_id != JOURNAL_FOLDER_ID {
                            partial_completion = true;
                            continue;
                        }
                        if request.move_copy_want_copy() {
                            match store
                                .upsert_mapi_journal_entry(UpsertJournalEntryInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    subject: entry.entry.subject.clone(),
                                    body_text: entry.entry.body_text.clone(),
                                    entry_type: entry.entry.entry_type.clone(),
                                    message_class: entry.entry.message_class.clone(),
                                    starts_at: entry.entry.starts_at.clone(),
                                    ends_at: entry.entry.ends_at.clone(),
                                    occurred_at: entry.entry.occurred_at.clone(),
                                    companies_json: entry.entry.companies_json.clone(),
                                    contacts_json: entry.entry.contacts_json.clone(),
                                })
                                .await
                            {
                                Ok(copied) => {
                                    if remember_created_mapi_identity(
                                        store,
                                        principal,
                                        MapiIdentityObjectKind::JournalEntry,
                                        copied.id,
                                        None,
                                        None,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        partial_completion = true;
                                    }
                                }
                                Err(_) => partial_completion = true,
                            }
                        }
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x33,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                if crate::mapi_store::recoverable_storage_folder(source_folder_id).is_some() {
                    if request.move_copy_want_copy() {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes)
                    else {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    };
                    let mut partial_completion = false;
                    for message_id in request.move_copy_message_ids() {
                        let Some(item) =
                            snapshot.recoverable_item_for_id(source_folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        if store
                            .restore_recoverable_item(
                                principal.account_id,
                                item.canonical_id,
                                Some(target_mailbox.id),
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-restore-recoverable-message".to_string(),
                                    subject: format!(
                                        "recoverable:{}->{}",
                                        item.canonical_id, target_mailbox.id
                                    ),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x33,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                if snapshot.public_folder_for_id(source_folder_id).is_some() {
                    let Some(target_folder) = snapshot.public_folder_for_id(target_folder_id)
                    else {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    };
                    let mut partial_completion = false;
                    for message_id in request.move_copy_message_ids() {
                        let Some(item) =
                            snapshot.public_folder_item_for_id(source_folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        let copied = store
                            .upsert_public_folder_item(
                                UpsertPublicFolderItemInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    public_folder_id: target_folder.folder.id,
                                    item_kind: item.item.item_kind.clone(),
                                    message_class: item.item.message_class.clone(),
                                    subject: item.item.subject.clone(),
                                    body_text: item.item.body_text.clone(),
                                    body_html_sanitized: item.item.body_html_sanitized.clone(),
                                    source_payload_json: item.item.source_payload_json.clone(),
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: if request.move_copy_want_copy() {
                                        "mapi-copy-public-folder-item".to_string()
                                    } else {
                                        "mapi-move-public-folder-item-copy".to_string()
                                    },
                                    subject: format!(
                                        "{}->{}",
                                        item.item.id, target_folder.folder.id
                                    ),
                                },
                            )
                            .await;
                        let Ok(copied) = copied else {
                            partial_completion = true;
                            continue;
                        };
                        if remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::PublicFolderItem,
                            copied.id,
                            None,
                            None,
                        )
                        .await
                        .is_err()
                        {
                            partial_completion = true;
                            continue;
                        }
                        if !request.move_copy_want_copy()
                            && store
                                .delete_public_folder_item(
                                    principal.account_id,
                                    item.item.public_folder_id,
                                    item.item.id,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "mapi-move-public-folder-item-delete".to_string(),
                                        subject: item.item.id.to_string(),
                                    },
                                )
                                .await
                                .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                    if !partial_completion {
                        session.record_notification(MapiNotificationEvent::content(
                            source_folder_id,
                            None,
                        ));
                        session.record_notification(MapiNotificationEvent::content(
                            target_folder_id,
                            None,
                        ));
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x33,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.move_copy_message_ids() {
                    let Some(email) =
                        message_for_id(source_folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.move_copy_want_copy() {
                        store
                            .copy_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-copy-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x33,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetReceiveFolder) => {
                if !private_logon_request_handle(session, &handle_slots, &request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x26,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(folder_id) = request.set_receive_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x26,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                let Some(message_class) = request.set_receive_folder_message_class() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x26,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                if !valid_receive_folder_message_class(message_class) {
                    responses.extend_from_slice(&rop_error_response(
                        0x26,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let canonical_folder_id = receive_folder_id_for_message_class(message_class);
                if folder_id != canonical_folder_id {
                    responses.extend_from_slice(&rop_error_response(
                        0x26,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    requested_message_class = %message_class,
                    canonical_message_class =
                        %explicit_receive_folder_message_class(message_class),
                    canonical_folder_id = %format!("0x{canonical_folder_id:016x}"),
                    "rca debug mapi canonical set receive folder accepted"
                );
                responses.extend_from_slice(&rop_simple_success_response(&request));
            }
            Some(RopId::SetSearchCriteria) => {
                let Some(MapiObject::Folder { folder_id, .. }) =
                    input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x30,
                        request.response_handle_index(),
                        EC_SEARCH_UNSUPPORTED,
                    ));
                    continue;
                };
                let definition = snapshot
                    .search_folder_definition_for_folder_id(*folder_id)
                    .or_else(|| session.search_folder_definition(*folder_id));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x30",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = %format_optional_debug_handle(input_handle(&handle_slots, &request)),
                    folder_id = %format!("0x{folder_id:016x}"),
                    folder_role = debug_role_for_folder_id(*folder_id),
                    definition_found = definition.is_some(),
                    fallback_builtin_role = builtin_search_role_for_folder_id(*folder_id).unwrap_or(""),
                    search_criteria_scope = %format_debug_search_criteria_scope(&request),
                    message = "rca debug mapi set search criteria lookup"
                );
                let Some(definition) = definition else {
                    if builtin_search_role_for_folder_id(*folder_id).is_some() {
                        responses.extend_from_slice(&rop_error_response(
                            0x30,
                            request.response_handle_index(),
                            EC_SEARCH_ACCESS_DENIED,
                        ));
                        continue;
                    }
                    responses.extend_from_slice(&rop_error_response(
                        0x30,
                        request.response_handle_index(),
                        EC_SEARCH_NOT_FOUND,
                    ));
                    continue;
                };
                if definition.is_builtin {
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                    continue;
                }
                let criteria = match bounded_search_criteria_from_rop(
                    &request,
                    *folder_id,
                    Some(definition),
                    mailboxes,
                ) {
                    Ok(criteria) => criteria,
                    Err(error) => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            tenant_id = %principal.tenant_id,
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x30",
                            folder_id = %format!("0x{folder_id:016x}"),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            search_criteria_scope = %format_debug_search_criteria_scope(&request),
                            search_criteria_error = %format!("{error:#010x}"),
                            "rca debug mapi set search criteria rejected"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x30,
                            request.response_handle_index(),
                            error,
                        ));
                        continue;
                    }
                };
                let input = UpsertSearchFolderInput {
                    id: Some(definition.id),
                    account_id: principal.account_id,
                    display_name: definition.display_name.clone(),
                    result_object_kind: definition.result_object_kind.clone(),
                    scope_json: criteria.scope_json,
                    restriction_json: criteria.restriction_json,
                    excluded_folder_roles: definition.excluded_folder_roles.clone(),
                };
                match store.upsert_search_folder(input).await {
                    Ok(definition) => {
                        session.remember_search_folder_definition(*folder_id, definition);
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x30,
                        request.response_handle_index(),
                        EC_SEARCH_NOT_FOUND,
                    )),
                }
            }
            Some(RopId::GetSearchCriteria) => {
                let Some(MapiObject::Folder { folder_id, .. }) =
                    input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x31,
                        request.response_handle_index(),
                        EC_SEARCH_UNSUPPORTED,
                    ));
                    continue;
                };
                let definition = snapshot
                    .search_folder_definition_for_folder_id(*folder_id)
                    .or_else(|| session.search_folder_definition(*folder_id));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x31",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = %format_optional_debug_handle(input_handle(&handle_slots, &request)),
                    folder_id = %format!("0x{folder_id:016x}"),
                    folder_role = debug_role_for_folder_id(*folder_id),
                    definition_found = definition.is_some(),
                    fallback_builtin_role = builtin_search_role_for_folder_id(*folder_id).unwrap_or(""),
                    message = "rca debug mapi get search criteria lookup"
                );
                let Some(definition) = definition else {
                    if let Some((restriction, folder_ids, flags)) =
                        builtin_search_criteria_to_rop_for_folder_id(*folder_id)
                    {
                        responses.extend_from_slice(&get_search_criteria_response(
                            &request,
                            &restriction,
                            &folder_ids,
                            flags,
                        ));
                        continue;
                    }
                    responses.extend_from_slice(&rop_error_response(
                        0x31,
                        request.response_handle_index(),
                        EC_SEARCH_NOT_FOUND,
                    ));
                    continue;
                };
                match bounded_search_criteria_to_rop(
                    definition,
                    mailboxes,
                    request.get_search_criteria_use_unicode(),
                )
                .or_else(|error| builtin_search_criteria_to_rop(definition).ok_or(error))
                {
                    Ok((restriction, folder_ids, flags)) => responses.extend_from_slice(
                        &get_search_criteria_response(&request, &restriction, &folder_ids, flags),
                    ),
                    Err(error) => responses.extend_from_slice(&rop_error_response(
                        0x31,
                        request.response_handle_index(),
                        error,
                    )),
                }
            }
            Some(RopId::GetReceiveFolder) => {
                echo_input_handle_table = true;
                if !private_logon_request_handle(session, &handle_slots, &request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(message_class) = request.receive_folder_message_class() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                if !valid_receive_folder_message_class(message_class) {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let response_folder_id = receive_folder_id_for_message_class(message_class);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x27",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.response_handle_index(),
                    hierarchy_sync_completed = session.hierarchy_sync_completed(),
                    requested_message_class = %message_class,
                    response_message_class =
                        %explicit_receive_folder_message_class(message_class),
                    response_folder_id = %format!("0x{response_folder_id:016x}"),
                    response_folder_is_calendar =
                        response_folder_id == CALENDAR_FOLDER_ID,
                    expected_calendar_folder_id = "0x0000000000100001",
                    "rca debug mapi get receive folder resolution"
                );
                responses.extend_from_slice(&rop_get_receive_folder_response(
                    &request,
                    response_folder_id,
                    explicit_receive_folder_message_class(message_class),
                ));
                session.record_receive_folder_verification_passed();
                session.record_post_hierarchy_request_contract(
                    post_hierarchy_get_receive_folder_contract(message_class, response_folder_id),
                );
            }
            Some(RopId::SetReadFlags) => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x66,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                if request.want_asynchronous().is_none()
                    || !read_flags_are_valid(request.read_flags(), true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x66,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let unread = unread_from_read_flags(request.read_flags());
                let mut partial_completion = false;
                let message_ids = request.message_ids();
                if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
                    if let Some(unread) = unread {
                        let mut patches = Vec::new();
                        for message_id in message_ids {
                            let Some(item) =
                                snapshot.public_folder_item_for_id(folder_id, message_id)
                            else {
                                partial_completion = true;
                                continue;
                            };
                            patches.push(lpe_storage::PublicFolderPerUserStatePatch {
                                item_id: item.item.id,
                                is_read: !unread,
                                last_seen_change: Some(item.item.change_counter),
                                private_json: None,
                            });
                        }
                        if !patches.is_empty()
                            && store
                                .patch_public_folder_per_user_state(
                                    principal.account_id,
                                    folder.folder.id,
                                    &patches,
                                )
                                .await
                                .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                    responses.extend_from_slice(&rop_set_read_flags_response(
                        &request,
                        partial_completion,
                    ));
                    continue;
                }
                for message_id in message_ids {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        .or_else(|| {
                            emails
                                .iter()
                                .find(|email| mapi_item_id_matches(&email.id, message_id))
                        })
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if let Some(unread) = unread {
                        if store
                            .update_jmap_email_flags(
                                principal.account_id,
                                email.id,
                                Some(unread),
                                None,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-set-read-flags".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                }
                responses
                    .extend_from_slice(&rop_set_read_flags_response(&request, partial_completion));
            }
            Some(RopId::SynchronizationConfigure) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let sync_type = request.sync_type();
                if MapiSyncType::from_u8(sync_type).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    break;
                }
                let sync_send_options = request.sync_send_options();
                let sync_flags = request.sync_flags();
                let sync_extra_flags = request.sync_extra_flags();
                let sync_property_tags = request.sync_property_tags();
                if !property_tags_are_supported(&sync_property_tags) {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    break;
                }
                let sync_property_tags_hex = sync_property_tags
                    .iter()
                    .map(|tag| format!("0x{tag:08x}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let partial_item_requested = sync_send_options & SYNC_SEND_OPTION_PARTIAL_ITEM != 0;
                let recover_mode_requested = sync_send_options & SYNC_SEND_OPTION_RECOVER_MODE != 0;
                let partial_item_behavior = if partial_item_requested && sync_type == 0x01 {
                    "full-item-fallback"
                } else if partial_item_requested {
                    "ignored-non-content-sync"
                } else {
                    "not-requested"
                };
                let checkpoint_kind = sync_checkpoint_kind(sync_type);
                let checkpoint_mailbox_id =
                    sync_checkpoint_mailbox_id(folder_id, sync_type, mailboxes);
                log_calendar_identity_chain(
                    principal,
                    "sync_configure",
                    folder_id,
                    checkpoint_mailbox_id,
                    Some(sync_type),
                    Some(snapshot),
                );
                let folder_role = debug_role_for_folder_id(folder_id);
                let folder_container_class = debug_container_class_for_folder_id(folder_id);
                let checkpoint = match store
                    .fetch_mapi_sync_checkpoint(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let checkpoint_status = checkpoint
                    .as_ref()
                    .map(|checkpoint| {
                        hierarchy_checkpoint_status(checkpoint_kind, folder_id, checkpoint)
                    })
                    .unwrap_or("missing");
                let checkpoint_cursor_source = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("source"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let checkpoint_cursor_sync_root_folder_id = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("syncRootFolderId"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|id| format!("0x{id:016x}"))
                    .unwrap_or_default();
                let checkpoint_cursor_hierarchy_sync_version = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("hierarchySyncVersion"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|version| version.to_string())
                    .unwrap_or_default();
                let checkpoint_cursor_change_sequence = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or_default();
                let checkpoint_cursor_modseq = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_modseq)
                    .unwrap_or_default();
                let checkpoint = checkpoint.filter(|_| checkpoint_status == "usable");
                let since = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or(0);
                let changes = match store
                    .fetch_mapi_sync_changes(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                        since,
                    )
                    .await
                {
                    Ok(changes) => changes,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let all_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
                    sync_mailboxes_for_excluding_deleted(
                        folder_id,
                        sync_type,
                        mailboxes,
                        &session.deleted_advertised_special_folders,
                    ),
                    snapshot,
                    folder_id,
                    sync_type,
                );
                let state_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
                    sync_state_mailboxes_for_excluding_deleted(
                        folder_id,
                        sync_type,
                        mailboxes,
                        &session.deleted_advertised_special_folders,
                    ),
                    snapshot,
                    folder_id,
                    sync_type,
                );
                let all_sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                let all_special_sync_objects =
                    special_sync_objects_for(folder_id, sync_type, snapshot, principal.account_id);
                log_calendar_special_sync_objects(
                    principal,
                    folder_id,
                    sync_type,
                    &all_special_sync_objects,
                );
                log_special_sync_objects(
                    principal,
                    folder_id,
                    sync_type,
                    &all_special_sync_objects,
                );
                let available_sync_mailbox_count = all_sync_mailboxes.len();
                let available_sync_email_count = all_sync_emails.len();
                let available_special_sync_object_count = all_special_sync_objects.len();
                let (delta_sync_mailboxes, delta_sync_emails, delta_special_sync_objects) =
                    if checkpoint.is_some() {
                        let changed_special_ids =
                            changed_special_ids_for_folder(folder_id, snapshot, &changes);
                        (
                            changed_sync_mailboxes(
                                all_sync_mailboxes.clone(),
                                &changes.changed_mailbox_ids,
                            ),
                            changed_sync_emails(
                                all_sync_emails.clone(),
                                &changes.changed_message_ids,
                            ),
                            changed_special_sync_objects(
                                all_special_sync_objects.clone(),
                                &changed_special_ids,
                            ),
                        )
                    } else {
                        (
                            all_sync_mailboxes.clone(),
                            all_sync_emails.clone(),
                            all_special_sync_objects.clone(),
                        )
                    };
                let sync_attachment_facts = sync_attachment_facts_for_with_embedded_content(
                    store,
                    principal.account_id,
                    folder_id,
                    &all_sync_emails,
                    snapshot,
                )
                .await;
                let delta_attachment_facts = sync_attachment_facts_for_with_embedded_content(
                    store,
                    principal.account_id,
                    folder_id,
                    &delta_sync_emails,
                    snapshot,
                )
                .await;
                let aggregate_sync_emails = if sync_type == 0x02 {
                    emails.to_vec()
                } else {
                    all_sync_emails.clone()
                };
                let state_attachment_facts =
                    sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
                let aggregate_attachment_facts =
                    sync_attachment_facts_for(folder_id, &aggregate_sync_emails, snapshot);
                let mut deleted_message_ids = if checkpoint.is_some() {
                    mapi_message_ids_for_deleted_changes(
                        store,
                        principal,
                        &changes.deleted_message_ids,
                    )
                    .await
                    .unwrap_or_default()
                } else {
                    Vec::new()
                };
                if checkpoint.is_some() && checkpoint_kind == MapiCheckpointKind::Hierarchy {
                    deleted_message_ids.extend(changes.deleted_mailbox_object_ids.iter().copied());
                    deleted_message_ids
                        .extend(changes.deleted_search_folder_object_ids.iter().copied());
                }
                if checkpoint.is_some() && folder_id == NOTES_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::Note,
                            &changes.deleted_note_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                if checkpoint.is_some() && folder_id == JOURNAL_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::JournalEntry,
                            &changes.deleted_journal_entry_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                if checkpoint.is_some() {
                    deleted_message_ids.extend(
                        deleted_special_object_ids_for_folder(
                            store, principal, folder_id, snapshot, &changes,
                        )
                        .await,
                    );
                }
                if checkpoint.is_some() && folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::ConversationAction,
                            &changes.deleted_conversation_action_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                let state = mapi_mailstore::sync_state_token_with_special_objects(
                    sync_type,
                    sync_flags,
                    folder_id,
                    &state_sync_mailboxes,
                    &all_sync_emails,
                    &state_attachment_facts,
                    &all_special_sync_objects,
                );
                let initial_state = mapi_mailstore::initial_sync_state_stream(sync_type);
                let transfer_buffer =
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        principal.account_id,
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &all_sync_mailboxes,
                        &all_sync_emails,
                        &sync_attachment_facts,
                        &all_special_sync_objects,
                        &[],
                        mailboxes,
                        &state_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                    );
                mapi_mailstore::log_hierarchy_transfer_debug(
                    sync_type,
                    sync_flags,
                    sync_extra_flags,
                    folder_id,
                    &sync_property_tags,
                    &transfer_buffer,
                );
                let tenant_id_debug = principal.tenant_id.to_string();
                let account_id_debug = principal.account_id.to_string();
                mapi_mailstore::log_fai_content_sync_debug(
                    sync_type,
                    folder_id,
                    principal.account_id,
                    &all_special_sync_objects,
                    &transfer_buffer,
                    mapi_mailstore::FaiContentSyncDebugContext {
                        mailbox: principal.email.as_str(),
                        tenant: tenant_id_debug.as_str(),
                        account: account_id_debug.as_str(),
                        mapi_request_id: request_id,
                        request_rop_id: "0x70",
                        checkpoint_kind: checkpoint_kind.as_str(),
                        active_transfer_selection: "initial_full_candidate",
                    },
                );
                let incremental_transfer_buffer = checkpoint.as_ref().map(|_| {
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        principal.account_id,
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &delta_sync_mailboxes,
                        &delta_sync_emails,
                        &delta_attachment_facts,
                        &delta_special_sync_objects,
                        &deleted_message_ids,
                        mailboxes,
                        &state_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                    )
                });
                let checkpoint_delta_mailbox_count = delta_sync_mailboxes.len();
                let checkpoint_delta_email_count = delta_sync_emails.len();
                let checkpoint_delta_special_object_count = delta_special_sync_objects.len();
                let checkpoint_deleted_message_count = deleted_message_ids.len();
                let incremental_transfer_buffer_bytes = incremental_transfer_buffer
                    .as_ref()
                    .map(|buffer| buffer.len())
                    .unwrap_or_default();
                let checkpoint_delta_total_count = checkpoint_delta_mailbox_count
                    + checkpoint_delta_email_count
                    + checkpoint_delta_special_object_count
                    + checkpoint_deleted_message_count;
                let checkpoint_zero_delta =
                    checkpoint.is_some() && checkpoint_delta_total_count == 0;
                let checkpoint_incremental_response_candidate =
                    checkpoint.is_some() && incremental_transfer_buffer.is_some();
                let initial_checkpoint_delta_selected = checkpoint_kind
                    == MapiCheckpointKind::Hierarchy
                    && checkpoint_zero_delta
                    && checkpoint_incremental_response_candidate;
                let initial_transfer_selection = if initial_checkpoint_delta_selected {
                    "checkpoint_delta_zero_delta_initial"
                } else if checkpoint_incremental_response_candidate {
                    "full_until_upload_state_delta_anchor"
                } else if checkpoint.is_some() {
                    "full_checkpoint_without_incremental_candidate"
                } else {
                    "full_no_checkpoint"
                };
                let scope_flags_present = sync_type != 0x01 || sync_flags & 0x0030 != 0;
                let default_fai_scope_requested = all_sync_emails.is_empty()
                    && all_special_sync_objects
                        .iter()
                        .all(|object| object.associated)
                    && all_special_sync_objects
                        .iter()
                        .any(|object| object.associated);
                let normal_scope_requested = sync_type != 0x01
                    || (scope_flags_present && sync_flags & 0x0020 != 0)
                    || (!scope_flags_present && !default_fai_scope_requested);
                let fai_scope_requested = sync_type != 0x01
                    || (scope_flags_present && sync_flags & 0x0010 != 0)
                    || (!scope_flags_present && default_fai_scope_requested);
                let wire_sync_email_count = if normal_scope_requested {
                    all_sync_emails.len()
                } else {
                    0
                };
                let wire_sync_special_object_count = all_special_sync_objects
                    .iter()
                    .filter(|object| {
                        if object.associated {
                            fai_scope_requested
                        } else {
                            normal_scope_requested
                        }
                    })
                    .count();
                let suppressed_normal_sync_object_count =
                    all_sync_emails.len().saturating_sub(wire_sync_email_count)
                        + all_special_sync_objects
                            .iter()
                            .filter(|object| !object.associated && !normal_scope_requested)
                            .count();
                let suppressed_fai_sync_object_count = all_special_sync_objects
                    .iter()
                    .filter(|object| object.associated && !fai_scope_requested)
                    .count();
                let checkpoint_store_allowed = suppressed_normal_sync_object_count == 0
                    && (suppressed_fai_sync_object_count == 0
                        || (normal_scope_requested && !fai_scope_requested));
                let checkpoint_skip_reason = if checkpoint_store_allowed {
                    ""
                } else {
                    "partial_content_scope_suppressed_objects"
                };
                let empty_content_sync_state_only = sync_type == 0x01
                    && wire_sync_email_count == 0
                    && wire_sync_special_object_count == 0
                    && checkpoint_deleted_message_count == 0
                    && incremental_transfer_buffer_bytes == transfer_buffer.len()
                    && transfer_buffer.len() == state.len().saturating_add(4);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x70",
                    folder_id = format_args!("0x{folder_id:016x}"),
                    folder_role,
                    folder_container_class,
                    sync_type = format_args!("0x{sync_type:02x}"),
                    sync_send_options = format_args!("0x{sync_send_options:02x}"),
                    sync_send_options_partial_item = partial_item_requested,
                    sync_send_options_recover_mode = recover_mode_requested,
                    sync_partial_item_behavior = partial_item_behavior,
                    sync_flags = format_args!("0x{sync_flags:04x}"),
                    sync_extra_flags = format_args!("0x{sync_extra_flags:08x}"),
                    sync_property_tag_count = sync_property_tags.len(),
                    sync_property_tags = %sync_property_tags_hex,
                    sync_property_filter_mode =
                        sync_property_filter_mode(sync_flags, &sync_property_tags),
                    checkpoint_loaded = checkpoint.is_some(),
                    checkpoint_kind = checkpoint_kind.as_str(),
                    checkpoint_mailbox_id = checkpoint_mailbox_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    checkpoint_scope = sync_checkpoint_scope(
                        folder_id,
                        checkpoint_mailbox_id,
                        &all_special_sync_objects
                    ),
                    checkpoint_status,
                    checkpoint_cursor_source,
                    checkpoint_cursor_sync_root_folder_id = %checkpoint_cursor_sync_root_folder_id,
                    checkpoint_cursor_hierarchy_sync_version =
                        %checkpoint_cursor_hierarchy_sync_version,
                    checkpoint_cursor_change_sequence,
                    checkpoint_cursor_modseq,
                    snapshot_mailbox_count = mailboxes.len(),
                    snapshot_email_count = emails.len(),
                    available_sync_mailbox_count,
                    available_sync_email_count,
                    available_special_sync_object_count,
                    sync_mailbox_count = all_sync_mailboxes.len(),
                    sync_state_mailbox_count = state_sync_mailboxes.len(),
                    sync_email_count = all_sync_emails.len(),
                    sync_special_object_count = all_special_sync_objects.len(),
                    normal_scope_requested,
                    fai_scope_requested,
                    wire_sync_email_count,
                    wire_sync_special_object_count,
                    empty_content_sync_state_only,
                    outlook_no_current_item_candidate = empty_content_sync_state_only,
                    suppressed_normal_sync_object_count,
                    suppressed_fai_sync_object_count,
                    checkpoint_store_allowed,
                    checkpoint_skip_reason,
                    checkpoint_delta_mailbox_count,
                    checkpoint_delta_email_count,
                    checkpoint_delta_special_object_count,
                    checkpoint_delta_total_count,
                    checkpoint_zero_delta,
                    checkpoint_incremental_response_candidate,
                    initial_checkpoint_delta_selected,
                    checkpoint_delta_selection_gate = "upload_state_delta_anchor",
                    initial_transfer_selection,
                    checkpoint_changed_contact_count = changes.changed_contact_ids.len(),
                    checkpoint_changed_calendar_event_count =
                        changes.changed_calendar_event_ids.len(),
                    checkpoint_changed_task_count = changes.changed_task_ids.len(),
                    checkpoint_deleted_contact_count = changes.deleted_contact_ids.len(),
                    checkpoint_deleted_calendar_event_count =
                        changes.deleted_calendar_event_ids.len(),
                    checkpoint_deleted_task_count = changes.deleted_task_ids.len(),
                    checkpoint_deleted_message_count,
                    current_change_sequence = changes.current_change_sequence,
                    initial_sync_state_bytes = initial_state.len(),
                    generated_sync_state_bytes = state.len(),
                    generated_sync_state_summary =
                        %mapi_mailstore::final_sync_state_debug_summary(&state),
                    transfer_buffer_bytes = transfer_buffer.len(),
                    incremental_transfer_buffer_bytes,
                    "rca debug mapi sync configure"
                );
                let active_transfer_buffer = if initial_checkpoint_delta_selected {
                    incremental_transfer_buffer
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| transfer_buffer.clone())
                } else {
                    transfer_buffer
                };
                let deferred_incremental_transfer_buffer = if initial_checkpoint_delta_selected {
                    None
                } else {
                    incremental_transfer_buffer
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: checkpoint_mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence: changes.current_change_sequence,
                        checkpoint_modseq: changes.current_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        checkpoint_zero_delta,
                        sync_type,
                        initial_state,
                        state,
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: deferred_incremental_transfer_buffer,
                        transfer_buffer: active_transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_configure_response(&request));
                output_handles.push(handle);
                content_sync_configure_observed = sync_type == 0x01;
            }
            Some(RopId::FastTransferSourceCopyMessages) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x4B,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let requested_ids = request.fast_transfer_message_ids();
                let mut selected = emails_for_folder(folder_id, mailboxes, emails)
                    .into_iter()
                    .filter(|email| requested_ids.contains(&mapi_message_id(email)))
                    .cloned()
                    .collect::<Vec<_>>();
                selected.sort_by(|left, right| left.id.cmp(&right.id));
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &selected, snapshot);
                let transfer_buffer =
                    mapi_mailstore::fast_transfer_message_list_buffer_with_attachments(
                        &selected,
                        &sync_attachment_facts,
                    );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        checkpoint_store_allowed: true,
                        checkpoint_skip_reason: "",
                        checkpoint_zero_delta: false,
                        sync_type: 0,
                        initial_state: Vec::new(),
                        state: Vec::new(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::FastTransferDestinationConfigure) => {
                let Some(target_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x53,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(folder_id) = session
                    .handles
                    .get(&target_handle)
                    .and_then(fast_transfer_destination_target_folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x53,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::FastTransferDestination {
                        folder_id,
                        target_handle,
                        buffer: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            Some(
                RopId::FastTransferDestinationPutBuffer
                | RopId::FastTransferDestinationPutBufferExtended,
            ) => {
                if first_fast_transfer_marker(&request).is_some() {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    break;
                }
                let upload_data = request.fast_transfer_upload_data().to_vec();
                let Some((target_handle, full_buffer)) =
                    staged_fast_transfer_destination_buffer(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match fast_transfer_property_values(&full_buffer) {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        break;
                    }
                };
                if !property_values.is_empty()
                    && apply_fast_transfer_destination_properties(
                        session,
                        target_handle,
                        property_values,
                    )
                    .is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                commit_fast_transfer_destination_buffer(
                    session,
                    &handle_slots,
                    &request,
                    full_buffer,
                );
                responses.extend_from_slice(&rop_fast_transfer_put_buffer_response(
                    &request,
                    upload_data.len(),
                ));
            }
            Some(
                RopId::FastTransferSourceCopyFolder
                | RopId::FastTransferSourceCopyTo
                | RopId::FastTransferSourceCopyProperties,
            ) => {
                let Some(object) = input_object(session, &handle_slots, &request).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((folder_id, transfer_buffer)) = fast_transfer_manifest_for_object(
                    request.rop_id,
                    &object,
                    principal.account_id,
                    mailboxes,
                    emails,
                    snapshot,
                ) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        checkpoint_store_allowed: true,
                        checkpoint_skip_reason: "",
                        checkpoint_zero_delta: false,
                        sync_type: 0,
                        initial_state: Vec::new(),
                        state: Vec::new(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::FastTransferSourceGetBuffer) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        checkpoint_zero_delta,
                        sync_type,
                        state,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let requested_buffer_bytes = request.fast_transfer_buffer_size();
                        let previous_transfer_position = *transfer_position;
                        let empty_content_sync_state_only = *sync_type == 0x01
                            && transfer_buffer.len() == state.len().saturating_add(4);
                        let upload_state_has_delta_anchor =
                            uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
                        let checkpoint_delta_available_before_get_buffer =
                            incremental_transfer_buffer.is_some();
                        let checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor =
                            checkpoint_delta_available_before_get_buffer
                                && !upload_state_has_delta_anchor;
                        let active_transfer_selection =
                            if *checkpoint_zero_delta && !checkpoint_delta_available_before_get_buffer
                            {
                                "checkpoint_delta_zero_delta_initial"
                            } else if checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor
                            {
                                "full_pending_upload_state_delta_anchor"
                            } else if upload_state_has_delta_anchor
                                && !checkpoint_delta_available_before_get_buffer
                            {
                                "checkpoint_delta_or_full_after_delta_anchor"
                            } else if checkpoint_delta_available_before_get_buffer {
                                "full_with_checkpoint_delta_available"
                            } else {
                                "full_or_static"
                            };
                        let response = rop_fast_transfer_source_get_buffer_response(
                            &request,
                            transfer_buffer,
                            transfer_position,
                        );
                        let completed = *transfer_position >= transfer_buffer.len();
                        let response_debug =
                            summarize_fast_transfer_get_buffer_response(&response, completed);
                        if completed && *sync_type == 0x02 {
                            let hierarchy_close_summary =
                                mapi_mailstore::hierarchy_transfer_close_summary(
                                    *sync_type,
                                    *folder_id,
                                    transfer_buffer,
                                );
                            let default_folder_hierarchy_membership_summary =
                                mapi_mailstore::default_folder_hierarchy_membership_summary(
                                    *sync_type,
                                    *folder_id,
                                    transfer_buffer,
                                );
                            completed_hierarchy_sync = Some((
                                *folder_id,
                                format!(
                                    "folder=0x{:016x};checkpoint_kind={};checkpoint_mailbox={};seq={};modseq={};state={};state_summary={};upload_buffer={};client_state={};upload_delta_anchor={};incremental={};delta_blocked_no_anchor={};selection={};requested={};response={};payload={};status={};completed={};position={}/{};{}",
                                    *folder_id,
                                    checkpoint_kind.as_str(),
                                    (*mailbox_id).map(|id| id.to_string()).unwrap_or_default(),
                                    *checkpoint_change_sequence,
                                    *checkpoint_modseq,
                                    state.len(),
                                    mapi_mailstore::final_sync_state_debug_summary(state),
                                    state_upload_buffer.len(),
                                    *client_state_uploaded_bytes,
                                    upload_state_has_delta_anchor,
                                    incremental_transfer_buffer.is_some(),
                                    checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor,
                                    active_transfer_selection,
                                    requested_buffer_bytes,
                                    response.len(),
                                    response_debug.transfer_payload_bytes,
                                    response_debug.transfer_status,
                                    completed,
                                    *transfer_position,
                                    transfer_buffer.len(),
                                    hierarchy_close_summary
                                ),
                                default_folder_hierarchy_membership_summary,
                            ));
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x4e",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_zero_delta = *checkpoint_zero_delta,
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            checkpoint_change_sequence = *checkpoint_change_sequence,
                            checkpoint_modseq = *checkpoint_modseq,
                            sync_state_bytes = state.len(),
                            sync_state_summary =
                                %mapi_mailstore::final_sync_state_debug_summary(state),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            upload_state_markers =
                                %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                            upload_state_has_delta_anchor =
                                upload_state_has_delta_anchor,
                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                            incremental_transfer_buffer_bytes = incremental_transfer_buffer
                                .as_ref()
                                .map(|buffer| buffer.len())
                                .unwrap_or_default(),
                            checkpoint_delta_selection_gate = "upload_state_delta_anchor",
                            checkpoint_delta_available_before_get_buffer,
                            checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor,
                            active_transfer_selection,
                            requested_buffer_bytes,
                            transfer_position_before = previous_transfer_position,
                            transfer_position_after = *transfer_position,
                            transfer_buffer_bytes = transfer_buffer.len(),
                            empty_content_sync_state_only,
                            outlook_no_current_item_candidate = empty_content_sync_state_only,
                            transfer_chunk_bytes =
                                (*transfer_position).saturating_sub(previous_transfer_position),
                            transfer_completed = completed,
                            transfer_status = if completed { "0x0003" } else { "0x0001" },
                            get_buffer_response_bytes = response.len(),
                            get_buffer_response_header_bytes = response_debug.header_bytes,
                            get_buffer_response_rop_id = %response_debug.rop_id,
                            get_buffer_response_rop_id_matches = response_debug.rop_id_matches,
                            get_buffer_response_handle_index = response_debug.handle_index,
                            get_buffer_return_value = %response_debug.return_value,
                            get_buffer_transfer_status_wire = %response_debug.transfer_status,
                            get_buffer_transfer_status_matches_completed =
                                response_debug.transfer_status_matches_completed,
                            get_buffer_in_progress_count = response_debug.in_progress_count,
                            get_buffer_total_step_count = response_debug.total_step_count,
                            get_buffer_reserved_byte = response_debug.reserved_byte,
                            get_buffer_reserved_zero = response_debug.reserved_zero,
                            get_buffer_transfer_buffer_size_wire =
                                response_debug.transfer_buffer_size,
                            get_buffer_transfer_payload_bytes = response_debug.transfer_payload_bytes,
                            get_buffer_transfer_buffer_size_matches_payload =
                                response_debug.transfer_buffer_size_matches_payload,
                            get_buffer_transfer_payload_preview_hex =
                                %response_debug.transfer_payload_preview_hex,
                            get_buffer_transfer_payload_tail_hex =
                                %response_debug.transfer_payload_tail_hex,
                            get_buffer_response_parse_error = %response_debug.parse_error,
                            "rca debug mapi fast transfer get buffer"
                        );
                        mapi_mailstore::log_hierarchy_get_buffer_payload_summary(
                            *sync_type,
                            *folder_id,
                            if completed { "0x0003" } else { "0x0001" },
                            transfer_buffer,
                        );
                        let checkpoint = (
                            *mailbox_id,
                            *checkpoint_kind,
                            *checkpoint_change_sequence,
                            *checkpoint_modseq,
                            *sync_type,
                            *folder_id,
                        );
                        responses.extend_from_slice(&response);
                        if completed && matches!(checkpoint.4, 0x01 | 0x02) {
                            let mut cursor_json = serde_json::json!({
                                "syncType": checkpoint.4,
                                "syncRootFolderId": checkpoint.5,
                                "source": "emsmdb-ics-download"
                            });
                            if checkpoint.1 == MapiCheckpointKind::Hierarchy {
                                cursor_json["hierarchySyncVersion"] =
                                    serde_json::json!(HIERARCHY_SYNC_CURSOR_VERSION);
                            }
                            if checkpoint.1 != MapiCheckpointKind::Hierarchy
                                && checkpoint.0.is_none()
                            {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    mapi_request_id = request_id,
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    folder_role = debug_role_for_folder_id(*folder_id),
                                    folder_container_class =
                                        debug_container_class_for_folder_id(*folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = "",
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "skipped_no_mailbox_id",
                                    checkpoint_skip_reason =
                                        "content_or_read_state_sync_without_canonical_mailbox_id",
                                    "rca debug mapi sync checkpoint store"
                                );
                                session.record_completed_sync_checkpoint(
                                    checkpoint.5,
                                    debug_role_for_folder_id(checkpoint.5),
                                    debug_container_class_for_folder_id(checkpoint.5),
                                    checkpoint.1.as_str(),
                                    checkpoint.4,
                                    "skipped_no_mailbox_id",
                                );
                            } else if !*checkpoint_store_allowed {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    mapi_request_id = request_id,
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    folder_role = debug_role_for_folder_id(*folder_id),
                                    folder_container_class =
                                        debug_container_class_for_folder_id(*folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = checkpoint
                                        .0
                                        .map(|id| id.to_string())
                                        .unwrap_or_default(),
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "not_stored_partial_scope",
                                    checkpoint_skip_reason = *checkpoint_skip_reason,
                                    "rca debug mapi sync checkpoint store"
                                );
                                session.record_completed_sync_checkpoint(
                                    checkpoint.5,
                                    debug_role_for_folder_id(checkpoint.5),
                                    debug_container_class_for_folder_id(checkpoint.5),
                                    checkpoint.1.as_str(),
                                    checkpoint.4,
                                    "ok_partial_scope_no_checkpoint",
                                );
                            } else {
                                let checkpoint_result = store
                                    .store_mapi_sync_checkpoint(
                                        principal.account_id,
                                        checkpoint.0,
                                        checkpoint.1,
                                        checkpoint.2,
                                        checkpoint.3,
                                        cursor_json,
                                    )
                                    .await;
                                match checkpoint_result {
                                    Ok(stored_checkpoint) => {
                                        tracing::info!(
                                            rca_debug = true,
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            request_type = "Execute",
                                            mapi_request_id = request_id,
                                            request_rop_id = "0x4e",
                                            folder_id = format_args!("0x{:016x}", *folder_id),
                                            folder_role = debug_role_for_folder_id(*folder_id),
                                            folder_container_class =
                                                debug_container_class_for_folder_id(*folder_id),
                                            sync_type = format_args!("0x{:02x}", checkpoint.4),
                                            checkpoint_kind = checkpoint.1.as_str(),
                                            checkpoint_mailbox_id = checkpoint
                                                .0
                                                .map(|id| id.to_string())
                                                .unwrap_or_default(),
                                            checkpoint_change_sequence = checkpoint.2,
                                            checkpoint_modseq = checkpoint.3,
                                            stored_change_sequence = stored_checkpoint.last_change_sequence,
                                            stored_modseq = stored_checkpoint.last_modseq,
                                            sync_state_bytes = state.len(),
                                            upload_state_buffer_bytes = state_upload_buffer.len(),
                                            upload_state_client_bytes = *client_state_uploaded_bytes,
                                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                            transfer_buffer_bytes = transfer_buffer.len(),
                                            transfer_position = *transfer_position,
                                            checkpoint_store_status = "ok",
                                            checkpoint_skip_reason = "",
                                            "rca debug mapi sync checkpoint store"
                                        );
                                        session.record_completed_sync_checkpoint(
                                            checkpoint.5,
                                            debug_role_for_folder_id(checkpoint.5),
                                            debug_container_class_for_folder_id(checkpoint.5),
                                            checkpoint.1.as_str(),
                                            checkpoint.4,
                                            "ok",
                                        );
                                    }
                                    Err(error) => {
                                        tracing::warn!(
                                            rca_debug = true,
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            request_type = "Execute",
                                            mapi_request_id = request_id,
                                            request_rop_id = "0x4e",
                                            folder_id = format_args!("0x{:016x}", *folder_id),
                                            folder_role = debug_role_for_folder_id(*folder_id),
                                            folder_container_class =
                                                debug_container_class_for_folder_id(*folder_id),
                                            sync_type = format_args!("0x{:02x}", checkpoint.4),
                                            checkpoint_kind = checkpoint.1.as_str(),
                                            checkpoint_mailbox_id = checkpoint
                                                .0
                                                .map(|id| id.to_string())
                                                .unwrap_or_default(),
                                            checkpoint_change_sequence = checkpoint.2,
                                            checkpoint_modseq = checkpoint.3,
                                            sync_state_bytes = state.len(),
                                            upload_state_buffer_bytes = state_upload_buffer.len(),
                                            upload_state_client_bytes = *client_state_uploaded_bytes,
                                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                            transfer_buffer_bytes = transfer_buffer.len(),
                                            transfer_position = *transfer_position,
                                            checkpoint_store_status = "error",
                                            checkpoint_skip_reason = "",
                                            error = %error,
                                            "rca debug mapi sync checkpoint store"
                                        );
                                        session.record_completed_sync_checkpoint(
                                            checkpoint.5,
                                            debug_role_for_folder_id(checkpoint.5),
                                            debug_container_class_for_folder_id(checkpoint.5),
                                            checkpoint.1.as_str(),
                                            checkpoint.4,
                                            "error",
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x4E,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::TellVersion) => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource { .. })
                | Some(MapiObject::SynchronizationCollector { .. })
                | Some(MapiObject::FastTransferDestination { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x86,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::SynchronizationUploadStateStreamBegin) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_property_tag,
                        state_upload_buffer,
                        ..
                    }) => {
                        let property_tag = request.upload_state_property_tag().unwrap_or_default();
                        let declared_bytes =
                            request.upload_state_transfer_size().unwrap_or_default();
                        *state_upload_property_tag = Some(property_tag);
                        state_upload_buffer.clear();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x75",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_declared_bytes = declared_bytes,
                            upload_state_empty_declared = declared_bytes == 0,
                            "rca debug mapi sync upload state begin"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_property_tag,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        ..
                    }) => {
                        let property_tag = request.upload_state_property_tag().unwrap_or_default();
                        let declared_bytes =
                            request.upload_state_transfer_size().unwrap_or_default();
                        *state_upload_property_tag = Some(property_tag);
                        state_upload_buffer.clear();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x75",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_declared_bytes = declared_bytes,
                            upload_state_empty_declared = declared_bytes == 0,
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            "rca debug mapi sync upload state begin"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x75,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamContinue) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_buffer,
                        ..
                    }) => {
                        let stream_data = request.stream_data();
                        state_upload_buffer.extend_from_slice(stream_data);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x76",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_chunk_bytes = stream_data.len(),
                            upload_state_chunk_preview = %hex_preview(stream_data, 16),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            "rca debug mapi sync upload state continue"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        ..
                    }) => {
                        let stream_data = request.stream_data();
                        state_upload_buffer.extend_from_slice(stream_data);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x76",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_chunk_bytes = stream_data.len(),
                            upload_state_chunk_preview = %hex_preview(stream_data, 16),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            "rca debug mapi sync upload state continue"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x76,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamEnd) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        checkpoint_zero_delta,
                        sync_type,
                        initial_state,
                        state,
                        state_upload_property_tag,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let uploaded_bytes = state_upload_buffer.len();
                        let upload_state_stream_summary = if uploaded_bytes == 0 {
                            "bytes=0;empty=true".to_string()
                        } else {
                            mapi_mailstore::replguid_globset_debug_summary(state_upload_buffer)
                        };
                        let property_tag = state_upload_property_tag.take().unwrap_or_default();
                        let upload_state_empty_stream_after_client_state =
                            uploaded_bytes == 0 && *client_state_uploaded_bytes > 0;
                        if uploaded_bytes > 0 {
                            mark_uploaded_state_stream(
                                client_state_uploaded_marker_mask,
                                property_tag,
                            );
                            let updated_initial_state =
                                mapi_mailstore::sync_state_stream_with_uploaded_property(
                                    *sync_type,
                                    initial_state,
                                    property_tag,
                                    state_upload_buffer,
                                );
                            *initial_state = updated_initial_state;
                        }
                        state_upload_buffer.clear();
                        *client_state_uploaded_bytes =
                            (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
                        let has_delta_anchor =
                            uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
                        if *client_state_uploaded_bytes > 0 && !has_delta_anchor {
                            *checkpoint_store_allowed = false;
                            *checkpoint_skip_reason = "uploaded_client_state_transfer";
                        } else if has_delta_anchor
                            && *checkpoint_skip_reason == "uploaded_client_state_transfer"
                        {
                            *checkpoint_store_allowed = true;
                            *checkpoint_skip_reason = "";
                        }
                        let mut selected_checkpoint_delta = false;
                        let checkpoint_delta_available_before_upload_state =
                            incremental_transfer_buffer.is_some();
                        if has_delta_anchor {
                            if let Some(buffer) = incremental_transfer_buffer.take() {
                                *transfer_buffer = buffer;
                                *transfer_position = 0;
                                selected_checkpoint_delta = true;
                            }
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x77",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_zero_delta = *checkpoint_zero_delta,
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_total_bytes = state.len(),
                            upload_state_stream_bytes = uploaded_bytes,
                            upload_state_empty_stream = uploaded_bytes == 0,
                            upload_state_empty_stream_expected = uploaded_bytes == 0,
                            upload_state_empty_stream_after_client_state,
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_stream_summary = %upload_state_stream_summary,
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            upload_state_markers =
                                %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                            upload_state_has_delta_anchor = has_delta_anchor,
                            checkpoint_delta_available_before_upload_state,
                            upload_state_selected_checkpoint_delta = selected_checkpoint_delta,
                            checkpoint_delta_selection_gate = "upload_state_delta_anchor",
                            checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor =
                                checkpoint_delta_available_before_upload_state && !has_delta_anchor,
                            active_transfer_selection = if selected_checkpoint_delta {
                                "checkpoint_delta_after_upload_state_delta_anchor"
                            } else if *checkpoint_zero_delta
                                && !checkpoint_delta_available_before_upload_state
                            {
                                "checkpoint_delta_zero_delta_initial"
                            } else if checkpoint_delta_available_before_upload_state {
                                "full_pending_upload_state_delta_anchor"
                            } else {
                                "full_or_static"
                            },
                            transfer_buffer_bytes = transfer_buffer.len(),
                            transfer_position = *transfer_position,
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        state,
                        state_upload_property_tag,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        sync_type,
                        ..
                    }) => {
                        let uploaded_bytes = state_upload_buffer.len();
                        let property_tag = state_upload_property_tag.take().unwrap_or_default();
                        if uploaded_bytes > 0 {
                            mark_uploaded_state_stream(
                                client_state_uploaded_marker_mask,
                                property_tag,
                            );
                        }
                        state_upload_buffer.clear();
                        *client_state_uploaded_bytes =
                            (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            mapi_request_id = request_id,
                            request_rop_id = "0x77",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_total_bytes = state.len(),
                            upload_state_stream_bytes = uploaded_bytes,
                            upload_state_empty_stream = uploaded_bytes == 0,
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            upload_state_markers =
                                %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                            upload_state_server_state_preserved = true,
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x77,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationOpenCollector) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x7E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id: sync_checkpoint_mailbox_id(
                            folder_id,
                            request.sync_type(),
                            mailboxes,
                        ),
                        checkpoint_kind: sync_checkpoint_kind(request.sync_type()),
                        sync_type: request.sync_type(),
                        state: Vec::new(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        uploaded_object_ids: Vec::new(),
                        uploaded_normal_change_numbers: Vec::new(),
                        uploaded_fai_change_numbers: Vec::new(),
                        uploaded_read_change_numbers: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::SynchronizationGetTransferState) => {
                let source_object = input_object(session, &handle_slots, &request);
                let Some((
                    folder_id,
                    mailbox_id,
                    checkpoint_kind,
                    checkpoint_change_sequence,
                    checkpoint_modseq,
                    checkpoint_store_allowed,
                    checkpoint_skip_reason,
                    sync_type,
                    state,
                )) = synchronization_context_state(source_object)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x82,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let transfer_buffer = if state.is_empty() && matches!(sync_type, 0x01 | 0x02) {
                    let sync_mailboxes = sync_mailboxes_for_excluding_deleted(
                        folder_id,
                        sync_type,
                        mailboxes,
                        &session.deleted_advertised_special_folders,
                    );
                    let sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                    let sync_attachment_facts =
                        sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
                    mapi_mailstore::sync_state_token_with_attachments(
                        sync_type,
                        folder_id,
                        &sync_mailboxes,
                        &sync_emails,
                        &sync_attachment_facts,
                    )
                } else {
                    state
                };
                let (
                    checkpoint_store_allowed,
                    checkpoint_skip_reason,
                    client_state_uploaded_bytes,
                    client_state_uploaded_marker_mask,
                ) = match source_object {
                    Some(MapiObject::SynchronizationCollector {
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        ..
                    }) if *client_state_uploaded_bytes > 0
                        && !uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask) =>
                    {
                        (
                            false,
                            "uploaded_client_state_transfer",
                            *client_state_uploaded_bytes,
                            *client_state_uploaded_marker_mask,
                        )
                    }
                    Some(MapiObject::SynchronizationCollector {
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        ..
                    }) => (
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        *client_state_uploaded_bytes,
                        *client_state_uploaded_marker_mask,
                    ),
                    _ => (checkpoint_store_allowed, checkpoint_skip_reason, 0, 0),
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        checkpoint_zero_delta: false,
                        sync_type,
                        initial_state: transfer_buffer.clone(),
                        state: transfer_buffer.clone(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_synchronization_get_transfer_state_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::SynchronizationImportMessageChange) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match request.import_property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let import_flag = request.import_flag().unwrap_or_default();
                let import_property_tags = property_values
                    .iter()
                    .map(|(tag, _)| format!("0x{tag:08x}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let import_source_key = property_values
                    .iter()
                    .find_map(|(tag, value)| match (*tag, value) {
                        (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => Some(bytes_to_hex(bytes)),
                        _ => None,
                    })
                    .unwrap_or_default();
                let import_source_key_global_counter =
                    imported_property_source_key_global_counter(&property_values);
                let import_source_key_identity_scope = import_source_key_global_counter
                    .map(import_source_key_identity_scope)
                    .unwrap_or("");
                let message_id = request.import_message_id().unwrap_or(0);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x72",
                    folder_id = format_args!("0x{:016x}", folder_id),
                    folder_role = debug_role_for_folder_id(folder_id),
                    folder_container_class = debug_container_class_for_folder_id(folder_id),
                    import_flag = format_args!("0x{import_flag:02x}"),
                    import_associated = import_flag & 0x10 != 0,
                    import_fail_on_conflict = import_flag & 0x40 != 0,
                    import_property_tag_count = property_values.len(),
                    import_property_tags = %import_property_tags,
                    import_source_key = %import_source_key,
                    import_source_key_global_counter = import_source_key_global_counter
                        .map(|counter| counter.to_string())
                        .unwrap_or_default(),
                    import_source_key_identity_scope,
                    parsed_message_id = format_args!("0x{message_id:016x}"),
                    "rca debug mapi sync import message change"
                );
                if import_flag & 0x10 != 0 && folder_id == COMMON_VIEWS_FOLDER_ID {
                    let properties = property_values.into_iter().collect::<HashMap<_, _>>();
                    let shortcut = navigation_shortcut_from_mapi_properties(
                        principal.account_id,
                        None,
                        &properties,
                    );
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x72",
                        folder_id = format_args!("0x{:016x}", folder_id),
                        decoded_shortcut =
                            %common_views_saved_shortcut_summary(&shortcut, &properties),
                        "rca debug mapi common views navigation shortcut import"
                    );
                    match store
                        .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                            id: None,
                            account_id: principal.account_id,
                            subject: shortcut.subject,
                            target_folder_id: shortcut.target_folder_id,
                            shortcut_type: shortcut.shortcut_type,
                            flags: shortcut.flags,
                            save_stamp: shortcut.save_stamp,
                            section: shortcut.section,
                            ordinal: shortcut.ordinal,
                            group_header_id: shortcut.group_header_id,
                            group_name: shortcut.group_name,
                        })
                        .await
                    {
                        Ok(saved) => {
                            let shortcut_id = match remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::NavigationShortcut,
                                saved.id,
                                None,
                                None,
                            )
                            .await
                            {
                                Ok(shortcut_id) => shortcut_id,
                                Err(_) => {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x72,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                            };
                            let handle = session.allocate_output_handle(
                                request.output_handle_index,
                                MapiObject::NavigationShortcut {
                                    folder_id,
                                    shortcut_id,
                                },
                            );
                            set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                            responses.extend_from_slice(
                                &rop_synchronization_import_message_change_response(&request),
                            );
                            output_handles.push(handle);
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                    continue;
                }
                if import_flag & 0x10 != 0 {
                    let pending_object = MapiObject::PendingAssociatedMessage {
                        folder_id,
                        properties: property_values.into_iter().collect(),
                    };
                    let handle =
                        session.allocate_output_handle(request.output_handle_index, pending_object);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                    continue;
                }
                if message_id != 0
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_some()
                {
                    let change_number = message_for_id(folder_id, message_id, mailboxes, emails)
                        .map(mapi_mailstore::canonical_message_change_number)
                        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message_id));
                    if import_flag & 0x40 != 0
                        && import_message_change_conflicts_with_current_pcl(
                            &property_values,
                            change_number,
                        )
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0109,
                        ));
                        continue;
                    }
                    if apply_canonical_message_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        mailboxes,
                        emails,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                            saved_email: None,
                            pending_properties: HashMap::new(),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        message_id,
                        change_number,
                        import_flag & 0x10 != 0,
                        import_flag & 0x10 == 0,
                    );
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else if message_id != 0
                    && snapshot
                        .public_folder_item_for_id(folder_id, message_id)
                        .is_some()
                {
                    if apply_canonical_public_folder_item_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::PublicFolderItem {
                            folder_id,
                            item_id: message_id,
                            properties: HashMap::new(),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        message_id,
                        mapi_mailstore::change_number_for_store_id(message_id),
                        false,
                        true,
                    );
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else if message_id != 0 && snapshot.note_for_id(folder_id, message_id).is_some() {
                    if apply_canonical_note_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        message_id,
                        mapi_mailstore::change_number_for_store_id(message_id),
                        false,
                        true,
                    );
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else if message_id != 0
                    && snapshot
                        .journal_entry_for_id(folder_id, message_id)
                        .is_some()
                {
                    if apply_canonical_journal_entry_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        message_id,
                        mapi_mailstore::change_number_for_store_id(message_id),
                        false,
                        true,
                    );
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else {
                    let pending_object = match folder_id {
                        NOTES_FOLDER_ID => MapiObject::PendingNote {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        _ => MapiObject::PendingMessage {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                            recipients: Vec::new(),
                        },
                    };
                    let handle =
                        session.allocate_output_handle(request.output_handle_index, pending_object);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                }
            }
            Some(RopId::SynchronizationImportHierarchyChange) => {
                let Some(_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
                let Some(display_name) = display_name else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if system_folder_display_name(&display_name) {
                    if let Some(existing) = imported_hierarchy_existing_mailbox(
                        &hierarchy_values,
                        &display_name,
                        mailboxes,
                    ) {
                        record_sync_upload_hierarchy_change(
                            session,
                            _folder_id,
                            mapi_folder_id(existing),
                        );
                    }
                    responses.extend_from_slice(
                        &rop_synchronization_import_hierarchy_change_response(&request),
                    );
                    continue;
                }
                if let Some(existing) =
                    imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
                {
                    if existing.role == "custom"
                        && existing.name.eq_ignore_ascii_case(&display_name)
                    {
                        record_sync_upload_hierarchy_change(
                            session,
                            _folder_id,
                            mapi_folder_id(existing),
                        );
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                    }
                    continue;
                }

                let parent_id =
                    imported_hierarchy_parent_mailbox_id(&hierarchy_values, _folder_id, mailboxes);
                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.clone(),
                            parent_id,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-hierarchy-change".to_string(),
                            subject: display_name.clone(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x73,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        record_sync_upload_hierarchy_change(
                            session,
                            _folder_id,
                            mapi_folder_id(&mailbox),
                        );
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(&request),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationImportDeletes) => {
                let hierarchy_collector_folder_id =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::SynchronizationCollector {
                            folder_id,
                            sync_type: 0x02,
                            ..
                        }) => Some(*folder_id),
                        _ => None,
                    };
                if let Some(collector_folder_id) = hierarchy_collector_folder_id {
                    let mut partial_completion = false;
                    for folder_id in request.import_delete_message_ids() {
                        let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                            partial_completion = true;
                            continue;
                        };
                        if mailbox.role != "custom" {
                            partial_completion = true;
                            continue;
                        }
                        if store
                            .destroy_jmap_mailbox(
                                principal.account_id,
                                mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-delete-folder".to_string(),
                                    subject: format!("folder:{}", mailbox.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                            continue;
                        }
                        record_sync_upload_hierarchy_change(
                            session,
                            collector_folder_id,
                            folder_id,
                        );
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x74,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x74,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                let hard_delete = request.import_delete_hard_delete();
                for message_id in request.import_delete_message_ids() {
                    let email = message_for_id(folder_id, message_id, mailboxes, emails);
                    if transient_client_local_message_id(message_id) && email.is_none() {
                        continue;
                    }
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = email else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if hard_delete
                        || email.mailbox_role == "trash"
                        || mailbox_is_trash_or_descendant(email.mailbox_id, mailboxes)
                    {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-hard-delete".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-soft-delete".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-delete-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    } else {
                        record_sync_upload_content_change(
                            session,
                            folder_id,
                            message_id,
                            mapi_mailstore::canonical_message_change_number(&email),
                            false,
                            false,
                        );
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x74,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SynchronizationImportMessageMove) => {
                let Some((source_folder_id, message_id)) = request.import_move() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(target_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if snapshot.note_for_id(source_folder_id, message_id).is_some() {
                    if target_folder_id == NOTES_FOLDER_ID {
                        record_sync_upload_content_checkpoint(session, source_folder_id);
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                if snapshot
                    .journal_entry_for_id(source_folder_id, message_id)
                    .is_some()
                {
                    if target_folder_id == JOURNAL_FOLDER_ID {
                        record_sync_upload_content_checkpoint(session, source_folder_id);
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .move_jmap_email_from_mailbox(
                        principal.account_id,
                        email.mailbox_id,
                        email.id,
                        target_mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-move".to_string(),
                            subject: format!("message:{}->{}", email.id, target_mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(moved) => {
                        match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            moved.id,
                            None,
                            None,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x78,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        record_sync_upload_content_checkpoint(session, source_folder_id);
                        record_sync_upload_content_change(
                            session,
                            target_folder_id,
                            crate::mapi::identity::mapped_mapi_object_id(&moved.id).unwrap_or(0),
                            mapi_mailstore::canonical_message_change_number(&moved),
                            false,
                            false,
                        );
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SynchronizationImportReadStateChanges) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x80,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for (message_id, unread) in request.import_read_state_changes() {
                    if transient_client_local_message_id(message_id) {
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-read-state".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    } else {
                        record_sync_upload_content_change(
                            session,
                            folder_id,
                            message_id,
                            mapi_mailstore::canonical_message_change_number(email),
                            false,
                            true,
                        );
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x80,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetLocalReplicaMidsetDeleted) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        initial_state,
                        state,
                        ..
                    }) => {
                        initial_state.extend_from_slice(request.local_replica_midset_deleted());
                        state.extend_from_slice(request.local_replica_midset_deleted());
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector { state, .. }) => {
                        state.extend_from_slice(request.local_replica_midset_deleted());
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x93,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::GetLocalReplicaIds) => {
                echo_input_handle_table = true;
                let (first_global_counter, _) = mapi_mailstore::local_replica_id_range(
                    principal.account_id,
                    request.local_replica_id_count(),
                    session.next_local_replica_sequence,
                );
                session.next_local_replica_sequence =
                    session.next_local_replica_sequence.saturating_add(1).max(1);
                responses.extend_from_slice(&rop_get_local_replica_ids_response(
                    &request,
                    first_global_counter,
                ));
            }
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders) => {
                if request.empty_folder_want_asynchronous().is_none()
                    || request.empty_folder_want_delete_associated().is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };

                if folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let result = if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
                    hard_delete_recoverable_folder_contents(store, principal, folder_id, snapshot)
                        .await
                } else if snapshot.public_folder_for_id(folder_id).is_some() {
                    hard_delete_public_folder_contents(store, principal, folder_id, snapshot).await
                } else if request.rop_id == RopId::HardDeleteMessagesAndSubfolders.as_u8() {
                    hard_delete_mailbox_tree_contents(
                        store, principal, folder_id, mailboxes, emails, snapshot,
                    )
                    .await
                } else {
                    hard_delete_folder_contents(
                        store, principal, folder_id, mailboxes, emails, snapshot,
                    )
                    .await
                };

                match result {
                    Ok((changed_folder_ids, partial_completion)) => {
                        for changed_folder_id in changed_folder_ids {
                            session.record_notification(MapiNotificationEvent::content(
                                changed_folder_id,
                                None,
                            ));
                        }
                        responses.extend_from_slice(&rop_partial_completion_response(
                            request.rop_id,
                            request.response_handle_index(),
                            partial_completion,
                        ));
                    }
                    Err(error) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        error,
                    )),
                }
            }
            Some(RopId::GetTransportFolder) => {
                responses.extend_from_slice(&transport_folder_response(
                    &request,
                    input_object(session, &handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::OptionsData) => {
                responses.extend_from_slice(&options_data_response(
                    &request,
                    input_object(session, &handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::GetReceiveFolderTable) => {
                if !private_logon_request_handle(session, &handle_slots, &request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x68,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x68",
                    row_count = 3u32,
                    first_message_class = "IPM.Appointment",
                    first_folder_id = format!("0x{CALENDAR_FOLDER_ID:016x}"),
                    calendar_row_present = true,
                    message_class_wire_type = "String8",
                    property_row_wire_shape =
                        "PidTagFolderId,PidTagMessageClass,PidTagLastModificationTime",
                    message = "rca debug mapi receive folder table"
                );
                responses.extend_from_slice(&get_receive_folder_table_response(&request));
                session.record_receive_folder_verification_passed();
            }
            Some(RopId::LongTermIdFromId) => {
                let source_id_bytes = request
                    .long_term_source_id_bytes()
                    .map(bytes_to_hex)
                    .unwrap_or_default();
                let decoded_object_id = request.long_term_source_object_id();
                let decoded_object_scope =
                    debug_object_scope_for_id(decoded_object_id, mailboxes, emails, snapshot);
                let response = rop_long_term_id_from_id_response_for_scope(
                    &request,
                    decoded_object_id,
                    decoded_object_scope,
                );
                let response_status = if response.len() > 6 {
                    "ok"
                } else {
                    "ecNotFound"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x43",
                    source_id_bytes = %source_id_bytes,
                    decoded_object_id = decoded_object_id
                        .map(|object_id| format!("{object_id:#018x}"))
                        .unwrap_or_default(),
                    decoded_advertised_special_folder = decoded_object_id
                        .map(is_advertised_special_folder)
                        .unwrap_or(false),
                    decoded_object_scope,
                    response_status,
                    message = "rca debug mapi long term id from id",
                );
                responses.extend_from_slice(&response)
            }
            Some(RopId::IdFromLongTermId) => {
                let replica_guid_aliases = [
                    *principal.account_id.as_bytes(),
                    principal.account_id.to_bytes_le(),
                ];
                let long_term_id = request.long_term_id();
                let decoded_object_id = long_term_id
                    .and_then(crate::mapi::identity::object_id_from_folder_identifier_bytes);
                let decoded_object_scope =
                    debug_object_scope_for_id(decoded_object_id, mailboxes, emails, snapshot);
                let response = rop_id_from_long_term_id_response(&request, &replica_guid_aliases);
                let response_status = if response.len() > 6 {
                    "ok"
                } else {
                    "ecNotFound"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x44",
                    microsoft_special_folder_open_rule =
                        "MS-OXOSFLD special-folder EntryIDs can be converted to FIDs by RopIdFromLongTermId before RopOpenFolder",
                    long_term_id_bytes = long_term_id.map(|bytes| bytes.len()).unwrap_or_default(),
                    long_term_id_preview = %long_term_id
                        .map(|bytes| hex_preview(bytes, 24))
                        .unwrap_or_default(),
                    decoded_object_id = decoded_object_id
                        .map(|object_id| format!("{object_id:#018x}"))
                        .unwrap_or_default(),
                    decoded_object_is_calendar = decoded_object_id == Some(CALENDAR_FOLDER_ID),
                    decoded_advertised_special_folder = decoded_object_id
                        .map(is_advertised_special_folder)
                        .unwrap_or(false),
                    decoded_object_scope,
                    response_status,
                    message = "rca debug mapi id from long term id",
                );
                responses.extend_from_slice(&response)
            }
            Some(RopId::GetPerUserLongTermIds) => {
                if !matches!(
                    input_object(session, &handle_slots, &request),
                    Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
                ) {
                    responses.extend_from_slice(&rop_error_response(
                        0x60,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let mut long_term_ids = snapshot
                    .public_folders()
                    .iter()
                    .filter_map(|folder| {
                        crate::mapi::identity::long_term_id_from_object_id(folder.id)
                    })
                    .collect::<Vec<_>>();
                if long_term_ids.is_empty() {
                    let mut canonical_folder_ids = Vec::new();
                    if let Ok(trees) = store.fetch_public_folder_trees(principal.account_id).await {
                        let mut pending_folder_ids = trees
                            .into_iter()
                            .filter_map(|tree| tree.root_folder_id)
                            .collect::<Vec<_>>();
                        let mut seen_folder_ids = HashSet::new();
                        while let Some(folder_id) = pending_folder_ids.pop() {
                            if !seen_folder_ids.insert(folder_id) {
                                continue;
                            }
                            if let Ok(folder) = store
                                .fetch_public_folder(principal.account_id, folder_id)
                                .await
                            {
                                canonical_folder_ids.push(folder.id);
                            }
                            if let Ok(children) = store
                                .fetch_public_folder_children(principal.account_id, folder_id)
                                .await
                            {
                                pending_folder_ids
                                    .extend(children.into_iter().map(|child| child.id));
                            }
                        }
                    }
                    let requests = canonical_folder_ids
                        .into_iter()
                        .map(|canonical_id| MapiIdentityRequest {
                            object_kind: MapiIdentityObjectKind::PublicFolder,
                            canonical_id,
                            reserved_global_counter: None,
                            source_key: None,
                        })
                        .collect::<Vec<_>>();
                    if let Ok(records) = store
                        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
                        .await
                    {
                        for record in records {
                            crate::mapi::identity::remember_mapi_identity_with_source_key(
                                record.canonical_id,
                                record.object_id,
                                Some(record.source_key),
                            );
                            if let Some(long_term_id) =
                                crate::mapi::identity::long_term_id_from_object_id(record.object_id)
                            {
                                long_term_ids.push(long_term_id);
                            }
                        }
                    }
                }
                responses.extend_from_slice(&rop_get_per_user_long_term_ids_response(
                    &request,
                    &long_term_ids,
                ));
            }
            Some(RopId::GetPerUserGuid) => {
                if !matches!(
                    input_object(session, &handle_slots, &request),
                    Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
                ) {
                    responses.extend_from_slice(&rop_error_response(
                        0x61,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(folder_id) = request
                    .long_term_id()
                    .and_then(crate::mapi::identity::object_id_from_long_term_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x61,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut public_folder_found = snapshot.public_folder_for_id(folder_id).is_some();
                if !public_folder_found {
                    if let Ok(records) = store
                        .fetch_mapi_identities_by_object_ids(principal.account_id, &[folder_id])
                        .await
                    {
                        for record in records {
                            if record.object_kind == MapiIdentityObjectKind::PublicFolder
                                && store
                                    .fetch_public_folder(principal.account_id, record.canonical_id)
                                    .await
                                    .is_ok()
                            {
                                public_folder_found = true;
                                break;
                            }
                        }
                    }
                }
                if !public_folder_found {
                    responses.extend_from_slice(&rop_error_response(
                        0x61,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_get_per_user_guid_response(
                    &request,
                    &crate::mapi::identity::STORE_REPLICA_GUID,
                ));
            }
            Some(RopId::ReadPerUserInformation) => {
                let Some(folder_id) = request.per_user_folder_object_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x63,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                let Some(public_folder) = snapshot.public_folder_for_id(folder_id) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x63,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                let states = match store
                    .fetch_public_folder_per_user_state(
                        principal.account_id,
                        public_folder.folder.id,
                    )
                    .await
                {
                    Ok(states) => states,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x63,
                            request.response_handle_index(),
                            EC_RULE_NOT_FOUND,
                        ));
                        continue;
                    }
                };
                let stream = public_folder_per_user_stream(&states);
                responses
                    .extend_from_slice(&rop_read_per_user_information_response(&request, &stream));
            }
            Some(RopId::WritePerUserInformation) => {
                let Some(folder_id) = request.per_user_folder_object_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x64,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                let Some(public_folder) = snapshot.public_folder_for_id(folder_id) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x64,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                if request.per_user_data_offset() != 0 || !request.per_user_has_finished() {
                    responses.extend_from_slice(&rop_error_response(
                        0x64,
                        request.response_handle_index(),
                        EC_RULE_INVALID_PARAMETER,
                    ));
                    continue;
                }
                let patches = match public_folder_per_user_patches(request.per_user_write_data()) {
                    Some(patches) => patches,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x64,
                            request.response_handle_index(),
                            EC_RULE_INVALID_PARAMETER,
                        ));
                        continue;
                    }
                };
                if !patches.is_empty()
                    && store
                        .patch_public_folder_per_user_state(
                            principal.account_id,
                            public_folder.folder.id,
                            &patches,
                        )
                        .await
                        .is_err()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x64,
                        request.response_handle_index(),
                        EC_RULE_INVALID_PARAMETER,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_write_per_user_information_response(&request));
            }
            Some(RopId::GetOwningServers) => {
                if !logon_request_handle(session, &handle_slots, &request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x42,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(folder_id) = request.public_folder_probe_object_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x42,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                if folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
                    && snapshot.public_folder_for_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x42,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                }
                let servers = snapshot.public_folder_replica_server_names(folder_id);
                responses.extend_from_slice(&rop_get_owning_servers_response(&request, &servers))
            }
            Some(RopId::PublicFolderIsGhosted) => {
                if !logon_request_handle(session, &handle_slots, &request) {
                    responses.extend_from_slice(&rop_error_response(
                        0x45,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(folder_id) = request.public_folder_probe_object_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x45,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                if folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
                    && snapshot.public_folder_for_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x45,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                }
                let is_ghosted = folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
                    && snapshot
                        .public_folder_replica_server_names(folder_id)
                        .is_empty();
                responses
                    .extend_from_slice(&rop_public_folder_is_ghosted_response(&request, is_ghosted))
            }
            Some(RopId::GetAddressTypes) => {
                echo_input_handle_table = true;
                let object = input_object(session, &handle_slots, &request);
                if object.is_none() {
                    responses.extend_from_slice(&address_types_response(&request, false));
                    continue;
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x49",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.response_handle_index(),
                    object_kind = mapi_object_debug_kind(object),
                    address_type_count = 2,
                    address_types = "EX,SMTP",
                    message = "rca debug mapi get address types",
                );
                responses.extend_from_slice(&address_types_response(&request, true));
            }
            Some(RopId::GetNamesFromPropertyIds) => {
                let property_ids = request.property_ids();
                let missing_property_ids = property_ids
                    .iter()
                    .copied()
                    .filter(|property_id| !session.named_property_ids.contains_key(property_id))
                    .collect::<Vec<_>>();
                if !missing_property_ids.is_empty() {
                    if let Ok(mappings) = store
                        .fetch_mapi_named_properties_by_ids(
                            principal.account_id,
                            &missing_property_ids,
                        )
                        .await
                    {
                        for mapping in mappings {
                            session.cache_named_property(mapping.property_id, mapping.property);
                        }
                    }
                }
                responses.extend_from_slice(&rop_get_names_from_property_ids_response(
                    &request, session,
                ));
            }
            Some(RopId::GetPropertyIdsFromNames) => {
                echo_input_handle_table = true;
                let properties = match request.named_property_names() {
                    Ok(properties) => properties,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x56,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                if properties.is_empty()
                    && matches!(
                        input_object(session, &handle_slots, &request),
                        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
                    )
                {
                    if let Ok(mappings) = store
                        .fetch_mapi_named_properties(principal.account_id, None)
                        .await
                    {
                        for mapping in mappings {
                            session.cache_named_property(mapping.property_id, mapping.property);
                        }
                    }
                    let property_ids = session
                        .named_properties_for_query(None)
                        .into_iter()
                        .map(|(property_id, _property)| property_id)
                        .collect::<Vec<_>>();
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x56",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = "logon",
                        create_missing = request.named_property_create(),
                        requested_named_property_count = 0,
                        requested_named_properties = "",
                        missing_named_property_count = 0,
                        missing_named_properties = "",
                        returned_property_id_count = property_ids.len(),
                        returned_property_ids = %format_debug_property_ids(&property_ids),
                        message = "rca debug mapi get property ids from names",
                    );
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                    continue;
                }
                let requested_named_properties = format_debug_named_properties(&properties);
                let mut property_ids = Vec::with_capacity(properties.len());
                let mut missing = Vec::new();
                for (index, property) in properties.iter().cloned().enumerate() {
                    match session.property_id_for_name(property.clone(), false) {
                        Some(property_id) => property_ids.push(property_id),
                        None => {
                            property_ids.push(0);
                            missing.push((index, property));
                        }
                    }
                }
                let missing_properties = missing
                    .iter()
                    .map(|(_index, property)| property.clone())
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    match store
                        .fetch_or_allocate_mapi_named_property_ids(
                            principal.account_id,
                            &missing_properties,
                            request.named_property_create(),
                        )
                        .await
                    {
                        Ok(mappings) => {
                            for (missing_index, (index, property)) in
                                missing.into_iter().enumerate()
                            {
                                let mapping = mappings.get(missing_index).cloned().flatten();
                                let property_id = mapping
                                    .map(|mapping| {
                                        cache_named_property_mapping_and_return_property_id(
                                            session,
                                            mapping.property_id,
                                            mapping.property,
                                        )
                                    })
                                    .or_else(|| {
                                        session.property_id_for_name(
                                            property,
                                            request.named_property_create(),
                                        )
                                    });
                                property_ids[index] = property_id.unwrap_or(0);
                            }
                        }
                        Err(_) if request.named_property_create() => {
                            responses.extend_from_slice(&rop_error_response(
                                0x56,
                                request.response_handle_index(),
                                0x8007_000E,
                            ));
                            continue;
                        }
                        Err(_) => {}
                    }
                }
                if !request.named_property_create() && property_ids.iter().any(|id| *id == 0) {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x56",
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = mapi_object_debug_kind(input_object(
                            session,
                            &handle_slots,
                            &request,
                        )),
                        create_missing = request.named_property_create(),
                        requested_named_property_count = properties.len(),
                        requested_named_properties = %requested_named_properties,
                        missing_named_property_count = missing_properties.len(),
                        missing_named_properties = %format_debug_named_properties(&missing_properties),
                        returned_property_id_count = property_ids.len(),
                        returned_property_ids = %format_debug_property_ids(&property_ids),
                        rop_return_value = "0x8004010f",
                        message = "rca debug mapi get property ids from names",
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x56,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x56",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.response_handle_index(),
                    object_kind = mapi_object_debug_kind(input_object(
                        session,
                        &handle_slots,
                        &request,
                    )),
                    create_missing = request.named_property_create(),
                    requested_named_property_count = properties.len(),
                    requested_named_properties = %requested_named_properties,
                    missing_named_property_count = missing_properties.len(),
                    missing_named_properties = %format_debug_named_properties(&missing_properties),
                    returned_property_id_count = property_ids.len(),
                    returned_property_ids = %format_debug_property_ids(&property_ids),
                    message = "rca debug mapi get property ids from names",
                );
                if contains_outlook_osc_contact_source_probe(&properties) {
                    session.record_outlook_view_failure_trace_event(format!(
                        "resolve_osc_contact_sources:request_id={request_id};object={};create_missing={};requested={};returned={}",
                        mapi_object_debug_kind(input_object(session, &handle_slots, &request)),
                        request.named_property_create(),
                        requested_named_properties,
                        format_debug_property_ids(&property_ids)
                    ));
                }
                responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                    &request,
                    &property_ids,
                ));
            }
            Some(RopId::QueryNamedProperties) => {
                if let Ok(mappings) = store
                    .fetch_mapi_named_properties(
                        principal.account_id,
                        request.named_property_query_guid(),
                    )
                    .await
                {
                    for mapping in mappings {
                        session.cache_named_property(mapping.property_id, mapping.property);
                    }
                }
                responses.extend_from_slice(&rop_query_named_properties_response(&request, session))
            }
            Some(RopId::RegisterNotification) => {
                let registration = notification_registration_from_request(&request);
                let input_handle_value = input_handle(&handle_slots, &request);
                let input_object = input_object(session, &handle_slots, &request);
                let input_object_kind = mapi_object_debug_kind(input_object);
                let input_folder_id = mapi_object_debug_folder_id(input_object);
                let input_context = format_handle_lineage_context(input_object);
                let notification_types = registration.notification_types;
                let notification_folder_id = registration.folder_id;
                if session.notification_cursor.is_none() {
                    session.notification_cursor = store
                        .fetch_mapi_notification_cursor(principal.account_id)
                        .await
                        .ok()
                        .flatten();
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::NotificationSubscription { registration },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_register_notification_response(&request));
                output_handles.push(handle);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x29",
                    request_rop_names = %request_rop_names,
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    input_handle_value = %format_optional_debug_handle(input_handle_value),
                    input_object_kind = input_object_kind,
                    input_folder_id = %input_folder_id,
                    input_context = %input_context,
                    output_handle_index = request.output_handle_index.unwrap_or(0),
                    output_handle_value = handle,
                    notification_types = %format!("0x{notification_types:04x}"),
                    want_whole_store = notification_folder_id.is_none(),
                    notification_folder_id = %notification_folder_id
                        .map(|folder_id| format!("0x{folder_id:016x}"))
                        .unwrap_or_else(|| "none".to_string()),
                    notification_cursor_loaded = session.notification_cursor.is_some(),
                    inbox_normal_contents_table_observed =
                        session.post_hierarchy_actions.inbox_normal_contents_table_observed,
                    inbox_normal_setcolumns_observed =
                        session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_setcolumns_observed,
                    inbox_normal_query_rows_observed =
                        session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_query_rows_observed,
                    last_normal_setcolumns_handle =
                        %format_optional_debug_handle(session
                            .post_hierarchy_actions
                            .last_inbox_normal_contents_table_setcolumns_handle),
                    last_normal_query_rows_handle =
                        %format_optional_debug_handle(session
                            .post_hierarchy_actions
                            .last_inbox_normal_contents_table_query_rows_handle),
                    recent_actions =
                        %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                    "rca debug mapi register notification"
                );
            }
            Some(RopId::GetPermissionsTable) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                    && !is_advertised_special_folder(folder_id)
                    && snapshot.public_folder_for_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x3E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    permission_table_object(folder_id),
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&get_permissions_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetRulesTable) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                    && snapshot.public_folder_for_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x3F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    rule_table_object(folder_id),
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&get_rules_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::ModifyPermissions) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                let mailbox_folder = folder_row_for_id(folder_id, mailboxes);
                let public_folder = snapshot.public_folder_for_id(folder_id);
                let calendar_collection_folder = snapshot
                    .collaboration_folder_for_id(folder_id)
                    .filter(|folder| folder.kind == MapiCollaborationFolderKind::Calendar);
                let default_calendar_folder = role_for_folder_id(folder_id) == Some("calendar");
                if mailbox_folder.is_none()
                    && public_folder.is_none()
                    && calendar_collection_folder.is_none()
                    && !default_calendar_folder
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x40,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                };
                let can_share = if default_calendar_folder {
                    true
                } else if let Some(folder) = calendar_collection_folder {
                    folder.collection.rights.may_share
                } else if let Some(public_folder) = public_folder {
                    public_folder.folder.rights.may_share
                } else {
                    snapshot
                        .permissions_for_folder(folder_id)
                        .iter()
                        .find(|permission| {
                            permission.member_account_id == Some(principal.account_id)
                        })
                        .is_some_and(|permission| may_share_from_rights(permission.rights))
                };
                if !can_share {
                    responses.extend_from_slice(&rop_error_response(
                        0x40,
                        request.response_handle_index(),
                        EC_SEARCH_ACCESS_DENIED,
                    ));
                    continue;
                }

                let rows = match request.modify_permissions_rows() {
                    Ok(rows) => rows,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x40,
                            request.response_handle_index(),
                            EC_RULE_INVALID_PARAMETER,
                        ));
                        continue;
                    }
                };
                let mut actions = Vec::new();
                let mut failed = None;
                for row in rows {
                    let row_kind = row.flags & (ROW_ADD | ROW_MODIFY | ROW_REMOVE);
                    if !matches!(row_kind, ROW_ADD | ROW_MODIFY | ROW_REMOVE) {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    }
                    let Some(member_id) = row
                        .properties
                        .get(&PID_TAG_MEMBER_ID)
                        .and_then(MapiValue::as_i64)
                        .and_then(|value| u64::try_from(value).ok())
                    else {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    };
                    if member_id == MEMBER_ID_DEFAULT || member_id == MEMBER_ID_ANONYMOUS {
                        continue;
                    }
                    let member_ids = [member_id];
                    let identity = match store
                        .fetch_mapi_identities_by_object_ids(principal.account_id, &member_ids)
                        .await
                    {
                        Ok(mut identities) => identities.pop(),
                        Err(_) => None,
                    };
                    let Some(identity) = identity
                        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Account)
                    else {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    };
                    if identity.canonical_id == principal.account_id {
                        continue;
                    }
                    let (may_read, may_write, may_delete, may_share) = if row_kind == ROW_REMOVE {
                        (false, false, false, false)
                    } else {
                        let Some(rights) = row
                            .properties
                            .get(&PID_TAG_MEMBER_RIGHTS)
                            .and_then(MapiValue::as_i64)
                            .and_then(|value| u32::try_from(value).ok())
                        else {
                            failed = Some(EC_RULE_INVALID_PARAMETER);
                            break;
                        };
                        let access = access_from_rights(rights);
                        (
                            access.may_read,
                            access.may_write,
                            access.may_delete,
                            may_share_from_rights(rights),
                        )
                    };
                    if !may_read && (may_write || may_delete || may_share) {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    }
                    if may_delete && !may_write {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    }
                    if may_share && !may_write {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    }
                    actions.push((
                        row_kind,
                        identity.canonical_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    ));
                }
                if let Some(error_code) = failed {
                    responses.extend_from_slice(&rop_error_response(
                        0x40,
                        request.response_handle_index(),
                        error_code,
                    ));
                    continue;
                }
                let mut failed = false;
                if default_calendar_folder {
                    for (
                        _row_kind,
                        grantee_account_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    ) in actions
                    {
                        if store
                            .set_mapi_calendar_permission(
                                principal.account_id,
                                grantee_account_id,
                                may_read,
                                may_write,
                                may_delete,
                                may_share,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-modify-calendar-permissions".to_string(),
                                    subject: format!("calendar {grantee_account_id}"),
                                },
                            )
                            .await
                            .is_err()
                        {
                            failed = true;
                            break;
                        }
                    }
                } else if let Some(folder) = calendar_collection_folder {
                    for (
                        _row_kind,
                        grantee_account_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    ) in actions
                    {
                        if store
                            .set_mapi_calendar_collection_permission(
                                folder.collection.owner_account_id,
                                &folder.collection.id,
                                grantee_account_id,
                                may_read,
                                may_write,
                                may_delete,
                                may_share,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-modify-calendar-permissions".to_string(),
                                    subject: format!(
                                        "calendar {} {}",
                                        folder.collection.id, grantee_account_id
                                    ),
                                },
                            )
                            .await
                            .is_err()
                        {
                            failed = true;
                            break;
                        }
                    }
                } else if let Some(folder) = mailbox_folder {
                    for (
                        _row_kind,
                        grantee_account_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    ) in actions
                    {
                        if store
                            .set_mapi_folder_permission(
                                principal.account_id,
                                folder.id,
                                grantee_account_id,
                                may_read,
                                may_write,
                                may_delete,
                                may_share,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-modify-permissions".to_string(),
                                    subject: format!(
                                        "folder {} {}",
                                        folder.name, grantee_account_id
                                    ),
                                },
                            )
                            .await
                            .is_err()
                        {
                            failed = true;
                            break;
                        }
                    }
                } else if let Some(folder) = public_folder {
                    for (
                        row_kind,
                        grantee_account_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    ) in actions
                    {
                        let audit = AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-modify-public-folder-permissions".to_string(),
                            subject: format!(
                                "public folder {} {}",
                                folder.folder.display_name, grantee_account_id
                            ),
                        };
                        let result = if row_kind == ROW_REMOVE {
                            store
                                .delete_public_folder_permission(
                                    principal.account_id,
                                    folder.folder.id,
                                    grantee_account_id,
                                    audit,
                                )
                                .await
                                .map(|_| ())
                        } else {
                            store
                                .upsert_public_folder_permission(
                                    PublicFolderPermissionInput {
                                        account_id: principal.account_id,
                                        public_folder_id: folder.folder.id,
                                        principal_account_id: grantee_account_id,
                                        may_read,
                                        may_write,
                                        may_delete,
                                        may_share,
                                    },
                                    audit,
                                )
                                .await
                                .map(|_| ())
                        };
                        if result.is_err() {
                            failed = true;
                            break;
                        }
                    }
                }
                if failed {
                    responses.extend_from_slice(&rop_error_response(
                        0x40,
                        request.response_handle_index(),
                        EC_RULE_INVALID_PARAMETER,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_modify_permissions_response(&request))
            }
            Some(RopId::ModifyRules) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x41,
                        request.response_handle_index(),
                        EC_RULE_NOT_FOUND,
                    ));
                    continue;
                }
                let rows = match request.modify_rules_rows() {
                    Ok(rows) => rows,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x41,
                            request.response_handle_index(),
                            EC_RULE_INVALID_PARAMETER,
                        ));
                        continue;
                    }
                };
                let mut failed = None;
                for row in rows {
                    let row_kind = row.flags & (ROW_ADD | ROW_MODIFY | ROW_REMOVE);
                    if row_kind == ROW_REMOVE {
                        let Some(rule_id) = row
                            .properties
                            .get(&PID_TAG_RULE_ID)
                            .and_then(MapiValue::as_i64)
                            .map(|value| value.max(0) as u64)
                        else {
                            failed = Some(EC_RULE_INVALID_PARAMETER);
                            break;
                        };
                        let Some(rule) = snapshot.rules().iter().find(|rule| rule.id == rule_id)
                        else {
                            failed = Some(EC_RULE_NOT_FOUND);
                            break;
                        };
                        if store
                            .delete_sieve_script(
                                principal.account_id,
                                &rule.name,
                                rule_audit(principal, "mapi.rule.delete", &rule.name),
                            )
                            .await
                            .is_err()
                        {
                            failed = Some(EC_RULE_NOT_FOUND);
                            break;
                        }
                        continue;
                    }
                    if row_kind != ROW_ADD && row_kind != ROW_MODIFY {
                        failed = Some(EC_RULE_UNSUPPORTED);
                        break;
                    }
                    let mutation = match bounded_rule_mutation_from_row(&row) {
                        Ok(mutation) => mutation,
                        Err(error) => {
                            failed = Some(error);
                            break;
                        }
                    };
                    if store
                        .put_sieve_script(
                            principal.account_id,
                            &mutation.name,
                            &mutation.content,
                            mutation.active,
                            rule_audit(principal, "mapi.rule.upsert", &mutation.name),
                        )
                        .await
                        .is_err()
                    {
                        failed = Some(EC_RULE_INVALID_PARAMETER);
                        break;
                    }
                }
                if let Some(error) = failed {
                    responses.extend_from_slice(&rop_error_response(
                        0x41,
                        request.response_handle_index(),
                        error,
                    ));
                } else {
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
            }
            Some(RopId::GetStoreState) => {
                responses.extend_from_slice(&store_state_response(
                    &request,
                    input_handle(&handle_slots, &request).is_some(),
                ));
            }
            Some(RopId::Abort) => {
                responses.extend_from_slice(&abort_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ));
            }
            Some(RopId::Progress) => {
                responses.extend_from_slice(&progress_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ));
            }
            Some(RopId::ResetTable) => {
                responses.extend_from_slice(&reset_table_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request)
                        .is_some_and(reset_table_state),
                ));
            }
            Some(RopId::FreeBookmark) => responses.extend_from_slice(&free_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
            Some(RopId::Logon) => {
                if let TypedRopRequest::Logon(logon_request) = &typed_request {
                    log_rop_logon_request_identity(principal, request_id, logon_request);
                }
                let logon_context = allocate_logon_response_context(
                    session,
                    &mut handle_slots,
                    principal,
                    &request,
                );
                if logon_context.is_private_logon {
                    responses.extend_from_slice(&rop_logon_response_body(principal, &request));
                    log_default_folder_discovery_contract(
                        principal,
                        request_id,
                        "private_logon_response",
                        "0xfe",
                        mailboxes,
                        emails,
                        snapshot,
                    );
                } else {
                    responses.extend_from_slice(&rop_public_folder_logon_response_body(
                        principal, &request,
                    ));
                }
                log_outlook_bootstrap_phase(
                    principal,
                    "logon_default_folder_ids_returned",
                    "0xfe",
                    None,
                    false,
                    None,
                    None,
                    Some(logon_context.handle),
                    &logon_context.special_folder_ids,
                );
                output_handles.push(logon_context.handle);
            }
            Some(rop_id) => {
                responses.extend_from_slice(&unsupported_known_rop_response(rop_id, &request));
            }
            None => {
                responses.extend_from_slice(&unsupported_unknown_rop_response(&request));
                break;
            }
        }
        if let Some((
            sync_root_folder_id,
            get_buffer_summary,
            default_folder_hierarchy_membership_summary,
        )) = completed_hierarchy_sync
        {
            session.record_completed_hierarchy_sync(
                sync_root_folder_id,
                get_buffer_summary,
                default_folder_hierarchy_membership_summary,
            );
        }
        if content_sync_configure_observed {
            session.record_content_sync_configure();
        }
        if typed_request.unsupported_is_terminal() {
            break;
        }
    }
    if !post_hierarchy_release_events.is_empty() {
        let post_hierarchy = post_hierarchy_action_summary(session, false);
        if post_hierarchy.content_sync_configure_observed {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_ids = %request_rop_ids,
                request_rop_names = %request_rop_names,
                request_non_release_rops = %request_non_release_rops,
                request_all_rops_are_release = request_all_rops_are_release,
                request_handle_count = request_handle_count,
                input_handle_table_summary = %request_handle_table_summary,
                release_rops_have_no_response_rows = true,
                response_rop_payload_bytes_before_handle_table = responses.len(),
                response_rop_payload_empty_is_expected = responses.is_empty(),
                last_completed_hierarchy_sync_root =
                    %post_hierarchy.last_completed_hierarchy_sync_root,
                content_sync_started_after_hierarchy =
                    post_hierarchy.content_sync_configure_observed,
                post_hierarchy_execute_count_before_record =
                    post_hierarchy.execute_count,
                released_handle_count = post_hierarchy_release_events.len(),
                released_handle_kinds =
                    %format_post_hierarchy_release_kinds(&post_hierarchy_release_events),
                released_handle_role_counts =
                    %post_sync_release_flags(&post_hierarchy_release_events),
                released_logon_after_content_sync = post_hierarchy_release_events
                    .iter()
                    .any(|event| matches!(
                        event.object_kind.as_str(),
                        "logon" | "public_folder_logon"
                    )),
                release_closes_all_live_handles = session.handles.is_empty(),
                remaining_live_handle_count = session.handles.len(),
                remaining_live_handles = %format_live_handle_debug_summary(session),
                release_context =
                    %format_post_hierarchy_release_context(&post_hierarchy_release_events),
                next_expected_client_step = "continue_mixed_sync_or_disconnect_after_release",
                "rca debug mapi post sync release-containing execute"
            );
        }
        if request_all_rops_are_release && post_hierarchy.content_sync_configure_observed {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_all_rops_are_release = request_all_rops_are_release,
                request_handle_count = request_handle_count,
                input_handle_table_summary = %request_handle_table_summary,
                release_rops_have_no_response_rows = true,
                response_rop_payload_bytes_before_handle_table = responses.len(),
                response_rop_payload_empty_is_expected = responses.is_empty(),
                last_completed_hierarchy_sync_root =
                    %post_hierarchy.last_completed_hierarchy_sync_root,
                content_sync_started_after_hierarchy =
                    post_hierarchy.content_sync_configure_observed,
                post_hierarchy_execute_count_before_record =
                    post_hierarchy.execute_count,
                released_handle_count = post_hierarchy_release_events.len(),
                released_handle_kinds =
                    %format_post_hierarchy_release_kinds(&post_hierarchy_release_events),
                released_handle_role_counts =
                    %post_sync_release_flags(&post_hierarchy_release_events),
                released_logon_after_content_sync = post_hierarchy_release_events
                    .iter()
                    .any(|event| matches!(
                        event.object_kind.as_str(),
                        "logon" | "public_folder_logon"
                    )),
                release_closes_all_live_handles = session.handles.is_empty(),
                remaining_live_handle_count = session.handles.len(),
                remaining_live_handles = %format_live_handle_debug_summary(session),
                release_context =
                    %format_post_hierarchy_release_context(&post_hierarchy_release_events),
                next_expected_client_step = "disconnect_or_reconnect_after_release_only_execute",
                "rca debug mapi post sync release-only execute"
            );
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            content_sync_started_after_hierarchy =
                post_hierarchy.content_sync_configure_observed,
            released_handle_count = post_hierarchy_release_events.len(),
            released_handle_kinds =
                %format_post_hierarchy_release_kinds(&post_hierarchy_release_events),
            released_logon_before_content_sync = post_hierarchy_release_events
                .iter()
                .any(|event| event.logon_before_content_sync),
            remaining_live_handle_count = session.handles.len(),
            release_context =
                %format_post_hierarchy_release_context(&post_hierarchy_release_events),
            "rca debug mapi post hierarchy close reason context"
        );
    }
    let response_handles = execute_response_handle_table(
        &responses,
        &handle_slots,
        &output_handles,
        echo_input_handle_table,
    );
    let response = if extended {
        rop_buffer_with_response_spec(responses, &response_handles)
    } else {
        rop_buffer_with_response(responses, &response_handles)
    };
    if extended {
        rpc_header_ext_rop_buffer(response)
    } else {
        response
    }
}

fn imported_message_source_key(properties: &HashMap<u32, MapiValue>) -> Option<Vec<u8>> {
    let source_key = match properties.get(&PID_TAG_SOURCE_KEY)? {
        MapiValue::Binary(bytes) => bytes,
        _ => return None,
    };
    (source_key.len() == 22 && source_key[..16] == crate::mapi::identity::STORE_REPLICA_GUID)
        .then(|| source_key.clone())
}

fn pending_message_is_sync_metadata_only(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> bool {
    !properties.is_empty()
        && recipients.is_empty()
        && properties.keys().all(|tag| {
            matches!(
                *tag,
                PID_TAG_SOURCE_KEY
                    | PID_TAG_LAST_MODIFICATION_TIME
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
}

fn pending_message_is_trash_sync_artifact(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> bool {
    folder_id == TRASH_FOLDER_ID
        && !properties.is_empty()
        && recipients.is_empty()
        && properties.keys().any(|tag| {
            matches!(
                *tag,
                PID_TAG_LAST_MODIFICATION_TIME
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
        && imported_message_source_key(properties)
            .as_deref()
            .and_then(source_key_global_counter)
            .is_some_and(|counter| {
                import_source_key_identity_scope(counter) == "out_of_lpe_persisted_range"
            })
}

fn imported_hierarchy_parent_mailbox_id(
    hierarchy_values: &[(u32, MapiValue)],
    collector_folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(bytes)) if *tag == PID_TAG_PARENT_SOURCE_KEY => {
                Some(bytes.as_slice())
            }
            _ => None,
        })
        .and_then(|parent_source_key| {
            mailboxes
                .iter()
                .find(|mailbox| {
                    mapi_mailstore::source_key_for_mailbox_folder(mailbox) == parent_source_key
                })
                .map(|mailbox| mailbox.id)
        })
        .or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mapi_folder_id(mailbox) == collector_folder_id)
                .map(|mailbox| mailbox.id)
        })
}

fn hierarchy_checkpoint_status(
    checkpoint_kind: MapiCheckpointKind,
    folder_id: u64,
    checkpoint: &MapiSyncCheckpoint,
) -> &'static str {
    if checkpoint_kind != MapiCheckpointKind::Hierarchy {
        return "usable";
    }
    if checkpoint
        .cursor_json
        .get("source")
        .and_then(serde_json::Value::as_str)
        != Some("emsmdb-ics-download")
    {
        return "stale-source";
    }
    if checkpoint
        .cursor_json
        .get("hierarchySyncVersion")
        .and_then(serde_json::Value::as_u64)
        != Some(HIERARCHY_SYNC_CURSOR_VERSION)
    {
        return "stale-version";
    }
    if checkpoint
        .cursor_json
        .get("syncRootFolderId")
        .and_then(serde_json::Value::as_u64)
        != Some(folder_id)
    {
        return "stale-root";
    }
    "usable"
}

fn sync_property_filter_mode(sync_flags: u16, requested_property_tags: &[u32]) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_max_rop_out_returns_buffer_too_small_response() {
        let request = [
            0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56,
            0x00, 0x00, 0x00,
        ];
        let response = rop_buffer_with_response_spec(vec![0x15, 0x01, 0, 0, 0, 0, 0, 0], &[0x56]);

        let capped = apply_execute_max_rop_out("test-request", &request, response.clone(), 4);

        assert_ne!(capped, response);
        assert_eq!(&capped[..3], &[0x0C, 0x00, 0xFF]);
        assert_eq!(&capped[3..5], &(response.len() as u16).to_le_bytes());
        assert_eq!(&capped[5..12], &[0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F]);
        assert_eq!(
            &capped[12..],
            &[0x6D, 0x00, 0x00, 0x00, 0x56, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn parse_execute_request_keeps_max_rop_out() {
        let rop_buffer = [0x02, 0x00];
        let mut body = Vec::new();
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
        body.extend_from_slice(&rop_buffer);
        body.extend_from_slice(&0x1234u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());

        let parsed = parse_execute_request(&body).unwrap();

        assert_eq!(parsed.rop_buffer, rop_buffer);
        assert_eq!(parsed.max_rop_out, 0x1234);
    }

    #[tokio::test]
    async fn execute_active_session_acquire_waits_for_short_outlook_overlap() {
        let session_id = format!("test-overlap-{}", Uuid::new_v4());
        let active = begin_active_session_request(&session_id).unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            drop(active);
        });

        let acquired = acquire_execute_active_session_request(&session_id).await;

        assert!(acquired.is_some());
        drop(acquired);
        release.await.unwrap();
        assert!(!session_request_is_active(&session_id));
    }

    #[test]
    fn release_only_execute_batch_is_store_independent() {
        let release_only = rop_buffer_with_response(vec![0x01, 0x00, 0x00], &[0x34]);
        assert!(rop_buffer_is_store_independent_release_only(&release_only));

        let mut release_then_getprops = vec![0x01, 0x00, 0x00];
        release_then_getprops.extend_from_slice(&[0x07, 0x00, 0x01]);
        release_then_getprops.extend_from_slice(&4096u16.to_le_bytes());
        release_then_getprops.extend_from_slice(&1u16.to_le_bytes());
        release_then_getprops.extend_from_slice(&0x3601_0003u32.to_le_bytes());
        let mixed = rop_buffer_with_response(release_then_getprops, &[0x34, 0xff]);
        assert!(!rop_buffer_is_store_independent_release_only(&mixed));
    }

    #[test]
    fn special_folder_getprops_probe_is_store_independent() {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn debug_named_property_context_reports_session_and_unresolved_properties() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x801f,
            MapiNamedProperty {
                guid: PS_PUBLIC_STRINGS_GUID,
                kind: MapiNamedPropertyKind::Name("custom field".to_string()),
            },
        );

        let context = format_debug_named_property_context(
            &session,
            &[0x801f_001f, PID_TAG_SUBJECT_W, 0x836b_001f],
        );

        assert!(context.contains("0x801f001f:id=0x801f:type=0x001f:source=session"));
        assert!(context.contains("name=custom field"));
        assert!(context.contains("0x836b001f:id=0x836b:type=0x001f:source=well_known"));
        assert!(context.contains("name=content-type"));
        assert!(!context.contains("0x0037001f"));
    }

    #[test]
    fn contents_table_named_property_context_reports_selected_columns() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x801f,
            MapiNamedProperty {
                guid: PS_PUBLIC_STRINGS_GUID,
                kind: MapiNamedPropertyKind::Name("view custom column".to_string()),
            },
        );
        let table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W, 0x801f_001f],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        let context = format_contents_table_named_property_context(&session, Some(&table));

        assert!(context.contains("0x801f001f:id=0x801f:type=0x001f:source=session"));
        assert!(context.contains("name=view custom column"));
        assert!(!context.contains("0x0037001f"));
    }

    #[test]
    fn smart_input_variant_resets_inbox_fai_cursor_before_query_rows() {
        let mut session = test_mapi_session();
        session.outlook_smart_input_variant = "fai_cursor_reset_before_query_rows".to_string();
        session.handles.insert(
            9,
            MapiObject::ContentsTable {
                folder_id: INBOX_FOLDER_ID,
                associated: true,
                columns: Vec::new(),
                columns_set: false,
                sort_orders: Vec::new(),
                category_count: 0,
                expanded_count: 0,
                collapsed_categories: HashSet::new(),
                restriction: None,
                bookmarks: HashMap::new(),
                next_bookmark: 1,
                position: 3,
            },
        );
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: vec![0x00, 0x01, 0x01, 0x00],
        };
        let handle_slots = vec![u32::MAX, u32::MAX, 9];

        let context = apply_outlook_smart_input_variant_before_query_rows(
            &mut session,
            &handle_slots,
            &request,
            "request:42",
            "QueryRows",
        )
        .expect("variant should apply");

        assert!(context.contains("previous_position=3"));
        assert!(session.outlook_smart_input_variant_applied);
        let Some(MapiObject::ContentsTable { position, .. }) = session.handles.get(&9) else {
            panic!("expected contents table");
        };
        assert_eq!(*position, 0);
    }

    #[test]
    fn inbox_view_handoff_table_contract_reports_folder_local_default_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_outlook_view_handoff_table_contract(
            INBOX_FOLDER_ID,
            true,
            &default_associated_config_columns(),
            &snapshot,
        );

        assert!(contract.contains("folder_local_default_supported=true"));
        assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
        assert!(contract.contains(&format!(
            "advertised_default_view_folder_id=0x{INBOX_FOLDER_ID:016x}"
        )));
        assert!(contract.contains(&format!(
            "expected_view_message_id=0x{:016x}",
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
        )));
    }

    #[test]
    fn inbox_fai_handoff_visibility_context_separates_prefix_and_named_view_rows() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let prefix_restriction = MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: "IPM.Configuration.".to_string(),
            fuzzy_level_low: 0x0002,
            fuzzy_level_high: 0x0001,
        };

        let context = format_inbox_fai_handoff_visibility_context(
            &snapshot,
            Some(&prefix_restriction),
            Uuid::nil(),
        );

        assert!(context.contains(&format!(
            "advertised_default_view_folder_id=0x{INBOX_FOLDER_ID:016x}"
        )));
        assert!(context.contains(&format!(
            "default_view_id=0x{:016x}",
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
        )));
        assert!(context.contains("exact_named_view_count=1"));
        assert!(context.contains("class=IPM.Microsoft.FolderDesign.NamedView"));
        assert!(context.contains("subject=Messages"));
    }

    #[test]
    fn junk_view_handoff_table_contract_reports_folder_local_default_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_outlook_view_handoff_table_contract(
            JUNK_FOLDER_ID,
            true,
            &default_associated_config_columns(),
            &snapshot,
        );

        assert!(contract.contains("folder_local_default_supported=true"));
        assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
        assert!(contract.contains(&format!(
            "advertised_default_view_folder_id=0x{JUNK_FOLDER_ID:016x}"
        )));
        assert!(contract.contains(&format!(
            "expected_view_message_id=0x{:016x}",
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
        )));
    }

    #[test]
    fn contacts_view_handoff_table_contract_reports_contact_default_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_outlook_view_handoff_table_contract(
            CONTACTS_FOLDER_ID,
            true,
            &default_associated_config_columns(),
            &snapshot,
        );

        assert!(contract.contains("folder_local_default_supported=true"));
        assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
        assert!(contract.contains(
            "visible_column_tags=0x001a001e,0x3001001e,0x8083001e,0x3a1c001e,0x3a16001e,0x3a17001e"
        ));
        assert!(contract.contains("expected_view_message_id=0x7fffffffffe90001"));
    }

    #[test]
    fn calendar_view_handoff_table_contract_reports_calendar_default_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_outlook_view_handoff_table_contract(
            CALENDAR_FOLDER_ID,
            true,
            &default_associated_config_columns(),
            &snapshot,
        );

        assert!(contract.contains("folder_local_default_supported=true"));
        assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
        assert!(contract.contains(
            "visible_column_tags=0x001a001e,0x0037001e,0x85160040,0x85170040,0x8208001e,0x82050003"
        ));
        assert!(contract.contains("expected_view_message_id=0x7fffffffffe90001"));
    }

    #[test]
    fn inbox_view_descriptor_set_columns_contract_reports_missing_descriptor_columns() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_inbox_view_descriptor_set_columns_behavior_contract(
            INBOX_FOLDER_ID,
            false,
            &[PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_DELIVERY_TIME],
            &snapshot,
        );

        assert!(contract.contains("phase=setcolumns"));
        assert!(contract.contains("default_view_id=0x7fffffffffe90001"));
        assert!(contract
            .contains("descriptor_columns=0x00170003,0x8514000b,0x001a001e,0x0e170003,0x0e1b000b"));
        assert!(!contract.contains("descriptor_columns=0x00040001"));
        assert!(contract.contains("selected_columns=0x0037001f,0x0e060040"));
        assert!(contract.ends_with("selected_missing_descriptor_columns="));
        assert!(!contract.contains("selected_missing_descriptor_columns=0x00040001"));
    }

    #[test]
    fn inbox_compact_descriptor_matches_observed_visible_projection() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let contract = format_inbox_view_descriptor_set_columns_behavior_contract(
            INBOX_FOLDER_ID,
            false,
            &[
                0x6748_0014,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_CREATION_TIME,
                PID_TAG_SUBJECT_W,
                PID_TAG_SENT_REPRESENTING_NAME_W,
                PID_TAG_MESSAGE_FLAGS,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_INTERNET_MESSAGE_ID_W,
                PID_TAG_IMPORTANCE,
                PID_TAG_HAS_ATTACHMENTS,
                PID_TAG_MESSAGE_STATUS,
                PID_LID_OUTLOOK_COMMON_8514_TAG,
                0x8017_000B,
                0x801F_001F,
                0x0041_0102,
                OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
                PID_TAG_MESSAGE_DELIVERY_TIME,
            ],
            &snapshot,
        );

        assert!(contract.contains("descriptor_columns=0x00170003"));
        assert!(contract.contains("0x0e1b000b"));
        assert!(contract.contains("0x0042001f"));
        assert!(
            contract.ends_with("selected_missing_descriptor_columns="),
            "{contract}"
        );
    }

    #[test]
    fn calendar_content_sync_changed_ids_are_projected() {
        let changed_event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
        let changes = MapiSyncChangeSet {
            changed_calendar_event_ids: vec![changed_event_id],
            ..Default::default()
        };

        let changed_ids = changed_special_ids_for_folder(
            CALENDAR_FOLDER_ID,
            &MapiMailStoreSnapshot::empty(),
            &changes,
        );

        assert_eq!(changed_ids, vec![changed_event_id]);
    }

    #[test]
    fn calendar_content_sync_changed_ids_include_associated_config() {
        let changed_event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
        let changed_config_id = Uuid::from_u128(0xc5a11c0ff1ce4c998b07111111111111);
        let changes = MapiSyncChangeSet {
            changed_calendar_event_ids: vec![changed_event_id],
            changed_associated_config_ids: vec![crate::store::MapiAssociatedConfigChange {
                folder_id: CALENDAR_FOLDER_ID,
                config_id: changed_config_id,
            }],
            ..Default::default()
        };

        let changed_ids = changed_special_ids_for_folder(
            CALENDAR_FOLDER_ID,
            &MapiMailStoreSnapshot::empty(),
            &changes,
        );

        assert_eq!(changed_ids, vec![changed_config_id, changed_event_id]);
    }

    #[test]
    fn table_columns_normalize_stale_sharing_named_property_alias() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x8fff,
            MapiNamedProperty {
                guid: PSETID_SHARING_GUID,
                kind: MapiNamedPropertyKind::Name(
                    "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
                ),
            },
        );

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8fff_0102, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![
                PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
                PID_TAG_SUBJECT_W
            ]
        );
    }

    #[test]
    fn table_columns_normalize_stale_sharing_alias_without_cached_mapping() {
        let session = test_mapi_session();

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8fff_0102, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![
                PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
                PID_TAG_SUBJECT_W
            ]
        );
    }

    #[test]
    fn table_columns_normalize_well_known_contact_email_named_property_alias() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x8022,
            MapiNamedProperty {
                guid: PSETID_ADDRESS_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_EMAIL_ADDRESS),
            },
        );

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8022_001f, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG, PID_TAG_SUBJECT_W]
        );
    }

    #[test]
    fn table_columns_normalize_outlook_contact_view_email_alias() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x8FFE,
            MapiNamedProperty {
                guid: PSETID_ADDRESS_GUID,
                kind: MapiNamedPropertyKind::Lid(
                    PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS,
                ),
            },
        );

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8FFE_001f, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG,
                PID_TAG_SUBJECT_W
            ]
        );
    }

    #[test]
    fn table_columns_normalize_outlook_visible_inbox_appointment_alias() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x8017,
            MapiNamedProperty {
                guid: PSETID_APPOINTMENT_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_APPOINTMENT_8F07),
            },
        );

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8017_000b, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG, PID_TAG_SUBJECT_W]
        );
    }

    #[test]
    fn table_columns_normalize_outlook_calendar_common_aliases() {
        let mut session = test_mapi_session();
        session.cache_named_property(
            0x8005,
            MapiNamedProperty {
                guid: PSETID_COMMON_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_SIDE_EFFECTS),
            },
        );
        session.cache_named_property(
            0x8013,
            MapiNamedProperty {
                guid: PSETID_COMMON_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_COMMON_8578),
            },
        );

        let columns = normalize_table_property_tags_for_session(
            &session,
            vec![0x8013_0003, 0x8005_0003, PID_TAG_SUBJECT_W],
        );

        assert_eq!(
            columns,
            vec![
                PID_LID_OUTLOOK_COMMON_8578_TAG,
                PID_LID_SIDE_EFFECTS_TAG,
                PID_TAG_SUBJECT_W
            ]
        );
    }

    #[test]
    fn get_property_ids_from_names_returns_canonical_well_known_id_from_stale_mapping() {
        let mut session = test_mapi_session();
        let property = MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        };

        let property_id = cache_named_property_mapping_and_return_property_id(
            &mut session,
            0x8fff,
            property.clone(),
        );

        assert_eq!(property_id, 0x8010);
        assert_eq!(session.property_name_for_id(0x8fff), property);
        assert_eq!(session.property_id_for_name(property, false), Some(0x8010));
    }

    #[test]
    fn get_property_ids_from_names_returns_canonical_contact_source_id_from_stale_mapping() {
        let mut session = test_mapi_session();
        let property = MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_SOURCE_80E0),
        };

        let property_id = cache_named_property_mapping_and_return_property_id(
            &mut session,
            0x80b8,
            property.clone(),
        );

        assert_eq!(property_id, PID_LID_OUTLOOK_CONTACT_SOURCE_80E0 as u16);
        assert_eq!(session.property_name_for_id(0x80b8), property);
        assert_eq!(
            session.property_id_for_name(property, false),
            Some(PID_LID_OUTLOOK_CONTACT_SOURCE_80E0 as u16)
        );
    }

    #[test]
    fn inbox_folder_type_getprops_probe_loads_store_snapshot() {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn inbox_display_name_getprops_probe_loads_store_snapshot() {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn root_folder_type_getprops_probe_stays_store_independent() {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn root_default_folder_entry_id_getprops_probe_loads_store_snapshot() {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn role_backed_special_folder_getprops_probes_load_store_snapshot() {
        for folder_id in [
            INBOX_FOLDER_ID,
            DRAFTS_FOLDER_ID,
            SENT_FOLDER_ID,
            TRASH_FOLDER_ID,
            OUTBOX_FOLDER_ID,
            CONTACTS_FOLDER_ID,
            SUGGESTED_CONTACTS_FOLDER_ID,
            QUICK_CONTACTS_FOLDER_ID,
            IM_CONTACT_LIST_FOLDER_ID,
            CONTACTS_SEARCH_FOLDER_ID,
            CALENDAR_FOLDER_ID,
            JOURNAL_FOLDER_ID,
            NOTES_FOLDER_ID,
            TASKS_FOLDER_ID,
            REMINDERS_FOLDER_ID,
            DOCUMENT_LIBRARIES_FOLDER_ID,
            SYNC_ISSUES_FOLDER_ID,
            CONFLICTS_FOLDER_ID,
            LOCAL_FAILURES_FOLDER_ID,
            SERVER_FAILURES_FOLDER_ID,
            JUNK_FOLDER_ID,
            RSS_FEEDS_FOLDER_ID,
            TRACKED_MAIL_PROCESSING_FOLDER_ID,
            TODO_SEARCH_FOLDER_ID,
            QUICK_STEP_SETTINGS_FOLDER_ID,
            ARCHIVE_FOLDER_ID,
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            CONVERSATION_HISTORY_FOLDER_ID,
        ] {
            let session = test_mapi_session();
            let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
            probe.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(folder_id).unwrap(),
            );
            probe.push(0);
            probe.extend_from_slice(&[0x07, 0x00, 0x01]);
            probe.extend_from_slice(&4096u16.to_le_bytes());
            probe.extend_from_slice(&1u16.to_le_bytes());
            probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
            let probe = rop_buffer_with_response(probe, &[u32::MAX]);

            assert!(
                !rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session),
                "folder {folder_id:#018x} must load store state"
            );
        }
    }

    #[test]
    fn special_folder_getprops_probe_rejects_custom_properties() {
        let session = test_mapi_session();
        let mut probe = vec![0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&0x9000_0003u32.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
    }

    #[test]
    fn folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name() {
        let principal = test_principal();
        let inbox_id = Uuid::from_u128(0x1111);
        crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
        let inbox = JmapMailbox {
            id: inbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 221,
            unread_emails: 17,
            size_octets: 0,
            is_subscribed: true,
        };

        let properties = folder_properties_for_open_from_mailboxes(
            &principal,
            INBOX_FOLDER_ID,
            &[inbox],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(
            properties.get(&PID_TAG_DISPLAY_NAME_W),
            Some(&MapiValue::String("Inbox".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTENT_COUNT),
            Some(&MapiValue::U32(221))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTENT_UNREAD_COUNT),
            Some(&MapiValue::U32(17))
        );
        assert_eq!(
            properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
            Some(&MapiValue::U32(associated_folder_message_count(
                INBOX_FOLDER_ID,
                &MapiMailStoreSnapshot::empty()
            )))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTAINER_CLASS_W),
            Some(&MapiValue::String("IPF.Note".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(&MapiValue::String("IPM.Note".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_RIGHTS),
            Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
        );
        assert_eq!(
            properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
            Some(&MapiValue::Binary(vec![0x01, 0x04, 0x00, 0x00, 0x10, 0x00]))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_VIEW_ENTRY_ID),
            crate::mapi::identity::message_entry_id_from_object_ids(
                principal.account_id,
                INBOX_FOLDER_ID,
                crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
            )
            .map(MapiValue::Binary)
            .as_ref()
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_FORM_FLAGS),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_WEBVIEWINFO),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_XVIEWINFO_E),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_VIEWS_ONLY),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_FORM_NAME_W),
            Some(&MapiValue::String(String::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_FORM_STORAGE),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_ACL_MEMBER_NAME_W),
            Some(&MapiValue::String(String::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_VIEWLIST_FLAGS),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_ARCHIVE_TAG),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_POLICY_TAG),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_RETENTION_PERIOD),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_RETENTION_FLAGS),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_ARCHIVE_PERIOD),
            Some(&MapiValue::U32(0))
        );
        let expected_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            principal.account_id,
            INBOX_FOLDER_ID,
        )
        .unwrap();
        assert_eq!(
            properties.get(&PID_TAG_ENTRY_ID),
            Some(&MapiValue::Binary(expected_entry_id))
        );
        assert_eq!(
            properties.get(&PID_TAG_RECORD_KEY),
            Some(&MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                INBOX_FOLDER_ID
            )))
        );
    }

    #[test]
    fn folder_properties_for_open_projects_search_folder_mail_class() {
        let principal = test_principal();

        let properties = folder_properties_for_open_from_mailboxes(
            &principal,
            SEARCH_FOLDER_ID,
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(
            properties.get(&PID_TAG_FOLDER_TYPE),
            Some(&MapiValue::U32(FOLDER_SEARCH))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTAINER_CLASS_W),
            Some(&MapiValue::String("IPF.Note".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(&MapiValue::String("IPM.Note".to_string()))
        );
    }

    #[test]
    fn folder_properties_for_open_projects_persisted_search_folder_contract() {
        let principal = test_principal();
        let definition_id = Uuid::parse_str("aaaaaaaa-3333-4333-8333-aaaaaaaaaaaa").unwrap();
        let folder_id = crate::mapi::identity::mapi_store_id(333);
        crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            SearchFolderDefinition {
                id: definition_id,
                account_id: principal.account_id,
                role: "category_red".to_string(),
                display_name: "Categorized Mail".to_string(),
                definition_kind: "user_saved".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: json!({"kind": "mapi_bounded"}),
                restriction_json: json!({"kind": "mapi_bounded"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: false,
            },
        ]);

        let properties =
            folder_properties_for_open_from_mailboxes(&principal, folder_id, &[], &snapshot);

        assert_eq!(
            properties.get(&PID_TAG_FOLDER_TYPE),
            Some(&MapiValue::U32(FOLDER_SEARCH))
        );
        assert_eq!(
            properties.get(&PID_TAG_DISPLAY_NAME_W),
            Some(&MapiValue::String("Categorized Mail".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_PARENT_FOLDER_ID),
            Some(&MapiValue::U64(SEARCH_FOLDER_ID))
        );
        assert_eq!(
            properties.get(&PID_TAG_RIGHTS),
            Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
        );
        let mut expected_extended_flags = extended_folder_flags();
        expected_extended_flags.extend_from_slice(&[0x03, 0x04]);
        expected_extended_flags.extend_from_slice(&0xAAAA_AAAAu32.to_le_bytes());
        expected_extended_flags.extend_from_slice(&[0x02, 0x10]);
        expected_extended_flags.extend_from_slice(definition_id.as_bytes());
        assert_eq!(
            properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
            Some(&MapiValue::Binary(expected_extended_flags))
        );
    }

    #[test]
    fn folder_properties_for_open_projects_collaboration_folder_contract() {
        let principal = test_principal();
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![lpe_storage::CollaborationCollection {
                id: "default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: principal.account_id,
                owner_email: principal.email.clone(),
                owner_display_name: principal.display_name.clone(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: lpe_storage::CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let properties = folder_properties_for_open_from_mailboxes(
            &principal,
            CALENDAR_FOLDER_ID,
            &[],
            &snapshot,
        );

        assert_eq!(
            properties.get(&PID_TAG_DISPLAY_NAME_W),
            Some(&MapiValue::String("Calendar".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTAINER_CLASS_W),
            Some(&MapiValue::String("IPF.Appointment".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(&MapiValue::String("IPM.Appointment".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTENT_COUNT),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_PARENT_FOLDER_ID),
            Some(&MapiValue::U64(IPM_SUBTREE_FOLDER_ID))
        );
        assert_eq!(
            properties.get(&PID_TAG_RIGHTS),
            Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
        );
        assert_eq!(
            properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
            Some(&MapiValue::Binary(extended_folder_flags()))
        );
        assert_eq!(
            properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
            Some(&MapiValue::U32(associated_folder_message_count(
                CALENDAR_FOLDER_ID,
                &snapshot
            )))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_FORM_FLAGS),
            Some(&MapiValue::U32(0))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_WEBVIEWINFO),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_FORM_NAME_W),
            Some(&MapiValue::String(String::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_ARCHIVE_TAG),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_RETENTION_PERIOD),
            Some(&MapiValue::U32(0))
        );
    }

    #[test]
    fn folder_properties_for_open_projects_public_folder_contract() {
        let principal = test_principal();
        let folder_id = PUBLIC_FOLDERS_ROOT_FOLDER_ID + 0x10000;
        let canonical_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777777);
        crate::mapi::identity::remember_mapi_identity(canonical_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::empty().with_public_folders(
            vec![lpe_storage::PublicFolder {
                id: canonical_id,
                tree_id: Uuid::from_u128(0x88888888_8888_4888_8888_888888888888),
                parent_folder_id: None,
                canonical_id,
                display_name: "Public Contacts".to_string(),
                folder_class: "IPF.Contact".to_string(),
                path: "/Public Contacts".to_string(),
                sort_order: 0,
                lifecycle_state: "active".to_string(),
                change_counter: 1,
                rights: lpe_storage::PublicFolderRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            }],
            Vec::new(),
            Vec::new(),
        );

        let properties =
            folder_properties_for_open_from_mailboxes(&principal, folder_id, &[], &snapshot);

        assert_eq!(
            properties.get(&PID_TAG_DISPLAY_NAME_W),
            Some(&MapiValue::String("Public Contacts".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_PARENT_FOLDER_ID),
            Some(&MapiValue::U64(PUBLIC_FOLDERS_ROOT_FOLDER_ID))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTAINER_CLASS_W),
            Some(&MapiValue::String("IPF.Contact".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(&MapiValue::String("IPM.Contact".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_RIGHTS),
            Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
        );
        assert_eq!(
            properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
            Some(&MapiValue::Binary(extended_folder_flags()))
        );
        assert_eq!(
            properties.get(&PID_TAG_FOLDER_WEBVIEWINFO),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_ARCHIVE_TAG),
            Some(&MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            properties.get(&PID_TAG_RETENTION_PERIOD),
            Some(&MapiValue::U32(0))
        );
    }

    #[test]
    fn folder_properties_for_open_projects_im_contact_list_default_post_class() {
        let principal = test_principal();

        let properties = folder_properties_for_open_from_mailboxes(
            &principal,
            IM_CONTACT_LIST_FOLDER_ID,
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(
            properties.get(&PID_TAG_CONTAINER_CLASS_W),
            Some(&MapiValue::String(
                "IPF.Contact.MOC.ImContactList".to_string()
            ))
        );
        assert_eq!(
            properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(&MapiValue::String("IPM.Contact".to_string()))
        );
    }

    #[test]
    fn advertised_special_folder_counts_snapshot_messages_when_mailbox_not_loaded() {
        let principal = test_principal();
        let draft_id = Uuid::from_u128(0x3333);
        crate::mapi::identity::remember_mapi_identity(draft_id, 0x0000_0000_01a4_0001);
        let draft = JmapEmail {
            id: draft_id,
            thread_id: Uuid::from_u128(0x4444),
            mailbox_ids: Vec::new(),
            mailbox_states: vec![test_mailbox_state(Uuid::from_u128(0x5555), "drafts")],
            mailbox_id: Uuid::from_u128(0x5555),
            mailbox_role: "drafts".to_string(),
            mailbox_name: "Drafts".to_string(),
            modseq: 1,
            received_at: "2026-06-11T13:41:41Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: None,
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Test Draft".to_string(),
            preview: String::new(),
            body_text: "Draft".to_string(),
            body_html_sanitized: None,
            unread: true,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 5,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            vec![draft],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let properties =
            folder_properties_for_open_from_mailboxes(&principal, DRAFTS_FOLDER_ID, &[], &snapshot);

        assert_eq!(
            properties.get(&PID_TAG_CONTENT_COUNT),
            Some(&MapiValue::U32(1))
        );
        assert_eq!(
            properties.get(&PID_TAG_CONTENT_UNREAD_COUNT),
            Some(&MapiValue::U32(1))
        );
    }

    fn test_mailbox_state(mailbox_id: Uuid, role: &str) -> lpe_storage::JmapEmailMailboxState {
        lpe_storage::JmapEmailMailboxState {
            mailbox_id,
            role: role.to_string(),
            name: role.to_string(),
            modseq: 1,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }
    }

    #[test]
    fn open_message_fallback_preserves_valid_requested_folder() {
        let inbox_id = Uuid::from_u128(0x1111);
        let sent_id = Uuid::from_u128(0x2222);
        crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(sent_id, SENT_FOLDER_ID);

        let mailboxes = vec![
            JmapMailbox {
                id: inbox_id,
                parent_id: None,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 1,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: sent_id,
                parent_id: None,
                role: "sent".to_string(),
                name: "Sent".to_string(),
                sort_order: 1,
                modseq: 1,
                total_emails: 1,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
        ];
        let email = JmapEmail {
            id: Uuid::from_u128(0x3333),
            thread_id: Uuid::from_u128(0x4444),
            mailbox_ids: vec![sent_id, inbox_id],
            mailbox_states: vec![
                test_mailbox_state(sent_id, "sent"),
                test_mailbox_state(inbox_id, "inbox"),
            ],
            mailbox_id: sent_id,
            mailbox_role: "sent".to_string(),
            mailbox_name: "Sent".to_string(),
            modseq: 1,
            received_at: "2026-06-07T19:00:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: None,
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Test".to_string(),
            preview: String::new(),
            body_text: String::new(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 0,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };

        assert_eq!(
            fallback_open_message_folder_id(INBOX_FOLDER_ID, &email, &mailboxes),
            INBOX_FOLDER_ID
        );
        assert_eq!(
            fallback_open_message_folder_id(TRASH_FOLDER_ID, &email, &mailboxes),
            SENT_FOLDER_ID
        );
    }

    #[test]
    fn normal_inbox_query_row_summary_reports_message_shapes() {
        let inbox_id = Uuid::from_u128(0x5555);
        crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
        let mailbox = JmapMailbox {
            id: inbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 1,
            size_octets: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: Uuid::from_u128(0x6666),
            thread_id: Uuid::from_u128(0x7777),
            mailbox_ids: vec![inbox_id],
            mailbox_states: vec![test_mailbox_state(inbox_id, "inbox")],
            mailbox_id: inbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 1,
            received_at: "2026-06-07T19:00:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Preview target".to_string(),
            preview: "Body text".to_string(),
            body_text: "Body text".to_string(),
            body_html_sanitized: Some("<p>Body text</p>".to_string()),
            unread: true,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 128,
            internet_message_id: Some("<message@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        crate::mapi::identity::remember_mapi_identity(
            email.id,
            crate::mapi::identity::mapi_store_id(0x6666),
        );

        let summary = format_normal_message_query_row_summary(
            INBOX_FOLDER_ID,
            false,
            0,
            true,
            50,
            &[],
            None,
            &[
                PID_TAG_MID,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_MESSAGE_FLAGS,
                PID_TAG_BODY_W,
                PID_TAG_RTF_COMPRESSED,
                PID_TAG_HTML_BINARY,
                PID_TAG_NATIVE_BODY,
                PID_TAG_INTERNET_MESSAGE_ID_W,
            ],
            std::slice::from_ref(&mailbox),
            std::slice::from_ref(&email),
            &empty_snapshot(),
        );

        assert!(summary.contains("total=1"));
        assert!(summary.contains("returned=1"));
        assert!(summary.contains("class=IPM.Note"));
        assert!(summary.contains("body_text_len=9"));
        assert!(summary.contains("body_html_len=16"));
        assert!(summary.contains("0x001a001f=IPM.Note"));
        assert!(summary.contains("0x0e070003="));
        assert!(summary.contains("0x1000001f=Body text"));
        assert!(summary.contains("0x10090102=binary:"));
        assert!(summary.contains("0x10130102=binary:bytes=16"), "{summary}");
        assert!(summary.contains("0x10160003=3"));
        assert!(summary.contains("0x1035001f=<message@example.test>"));

        let restricted = format_normal_message_query_row_summary(
            INBOX_FOLDER_ID,
            false,
            0,
            true,
            50,
            &[],
            Some(&MapiRestriction::Bitmask {
                property_tag: PID_TAG_MESSAGE_FLAGS,
                mask: 0x0000_0001,
                must_be_nonzero: true,
            }),
            &[PID_TAG_SUBJECT_W],
            std::slice::from_ref(&mailbox),
            std::slice::from_ref(&email),
            &empty_snapshot(),
        );

        assert!(restricted.contains("total=0"));
        assert!(restricted.contains("returned=0"));
    }

    #[test]
    fn hierarchy_query_rows_wire_summary_decodes_compact_folder_projection() {
        fn append_utf16z(row: &mut Vec<u8>, value: &str) {
            for unit in value.encode_utf16() {
                row.extend_from_slice(&unit.to_le_bytes());
            }
            row.extend_from_slice(&0u16.to_le_bytes());
        }

        let columns = vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_CONTENT_COUNT,
        ];
        let mut response = vec![0x15, 0, 0, 0, 0, 0, 0, 1, 0];
        response.push(0);
        response.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
        );
        append_utf16z(&mut response, "IPF.Note");
        append_utf16z(&mut response, "Inbox");
        response.extend_from_slice(&3u32.to_le_bytes());

        let summary = format_hierarchy_query_rows_wire_summary(&response, &columns, 8);

        assert!(summary.contains("total=1"), "{summary}");
        assert!(summary.contains("decoded=1"), "{summary}");
        assert!(
            summary.contains(
                "index=0;row_status=0x00;id=0x0000000000050001;class=IPF.Note;name=Inbox;count=3"
            ),
            "{summary}"
        );
        assert!(summary.contains("remaining_bytes=0"), "{summary}");
    }

    #[test]
    fn folder_properties_for_open_reports_inbox_associated_content_count() {
        let principal = test_principal();
        let inbox_id = Uuid::from_u128(0x2222);
        let config_id = Uuid::from_u128(0x3333);
        crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(config_id, 0x7fff_ffff_fffb_0001);
        let inbox = JmapMailbox {
            id: inbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 18,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: config_id,
                account_id: principal.account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.EAS".to_string(),
                subject: "IPM.Configuration.EAS".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let properties = folder_properties_for_open_from_mailboxes(
            &principal,
            INBOX_FOLDER_ID,
            &[inbox],
            &snapshot,
        );

        assert_eq!(
            properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
            Some(&MapiValue::U32(associated_folder_message_count(
                INBOX_FOLDER_ID,
                &snapshot
            )))
        );
    }

    #[test]
    fn outlook_special_folder_debug_classifiers_cover_configuration_folders() {
        assert_eq!(
            post_hierarchy_probe_folder_name(QUICK_STEP_SETTINGS_FOLDER_ID),
            "quick_step_settings"
        );
        assert_eq!(
            debug_container_class_for_folder_id(QUICK_STEP_SETTINGS_FOLDER_ID),
            "IPF.Configuration"
        );
        assert_eq!(
            debug_container_class_for_folder_id(CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
            "IPF.Configuration"
        );
        assert_eq!(
            debug_container_class_for_folder_id(RSS_FEEDS_FOLDER_ID),
            "IPF.Note.OutlookHomepage"
        );
        assert_eq!(
            debug_container_class_for_folder_id(SEARCH_FOLDER_ID),
            "IPF.Note"
        );
        assert_eq!(
            expected_special_folder_container_class(SEARCH_FOLDER_ID),
            "IPF.Note"
        );
        assert_eq!(
            expected_special_folder_item_message_class(SEARCH_FOLDER_ID),
            "IPM.Note"
        );
    }

    #[test]
    fn open_folder_debug_metadata_uses_real_dynamic_mailbox_values() {
        let mailbox_id = Uuid::from_u128(0x195);
        let folder_id = crate::mapi::identity::mapi_store_id(0x195);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "other".to_string(),
            name: "Categories Rename Search Folder".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };

        let (name, role, container_class) = debug_open_folder_metadata(folder_id, &[mailbox]);

        assert_eq!(name, "Categories Rename Search Folder");
        assert_eq!(role, "other");
        assert_eq!(container_class, "IPF.Note");
    }

    #[test]
    fn quick_step_synthetic_folder_allows_associated_message_creation() {
        assert!(synthetic_folder_allows_create_message(
            QUICK_STEP_SETTINGS_FOLDER_ID
        ));
        assert!(synthetic_folder_allows_create_message(
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        ));
        assert!(!synthetic_folder_allows_create_message(0x7777_0001));
    }

    #[test]
    fn freebusy_open_prefers_delegate_message_over_stale_associated_config_identity() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let delegate_id = Uuid::from_u128(0x64656c65_6761_7465_8000_000000000001);
        let stale_config_id = Uuid::from_u128(0x636f6e66_6967_6672_8000_000000000001);
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 311,
        );
        crate::mapi::identity::remember_mapi_identity(delegate_id, object_id);
        crate::mapi::identity::remember_mapi_identity(stale_config_id, object_id);

        let snapshot = MapiMailStoreSnapshot::empty()
            .with_delegate_freebusy_messages(vec![lpe_storage::DelegateFreeBusyMessageObject {
                id: delegate_id,
                account_id,
                owner_account_id: account_id,
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "Free/busy for owner@example.test".to_string(),
                body_text: "busy".to_string(),
                starts_at: None,
                ends_at: None,
                busy_status: None,
                payload_json: "{}".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            }])
            .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
                id: stale_config_id,
                account_id,
                folder_id: FREEBUSY_DATA_FOLDER_ID,
                message_class: "IPM.Configuration.FreeBusy".to_string(),
                subject: "Stale FreeBusy associated config".to_string(),
                properties_json: serde_json::json!({}),
            }]);

        let selected =
            delegate_freebusy_message_for_open(&snapshot, FREEBUSY_DATA_FOLDER_ID, object_id);

        assert_eq!(
            selected.map(|message| message.message.subject.as_str()),
            Some("Free/busy for owner@example.test")
        );
    }

    #[test]
    fn conversation_action_open_prefers_action_over_stale_associated_config_identity() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let action_id = Uuid::from_u128(0x636f6e76_6163_746e_8000_000000000001);
        let stale_config_id = Uuid::from_u128(0x636f6e66_6967_6361_8000_000000000001);
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 312,
        );
        crate::mapi::identity::remember_mapi_identity(action_id, object_id);
        crate::mapi::identity::remember_mapi_identity(stale_config_id, object_id);

        let snapshot = MapiMailStoreSnapshot::empty()
            .with_conversation_actions(vec![lpe_storage::ConversationAction {
                id: action_id,
                conversation_id: action_id,
                subject: "Conversation Action".to_string(),
                categories_json: "[]".to_string(),
                move_folder_entry_id: None,
                move_store_entry_id: None,
                move_target_mailbox_id: None,
                max_delivery_time: None,
                last_applied_time: None,
                version: lpe_storage::CONVERSATION_ACTION_VERSION,
                processed: 0,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            }])
            .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
                id: stale_config_id,
                account_id,
                folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                message_class: "IPM.Configuration.StaleConversationAction".to_string(),
                subject: "Stale Conversation Action associated config".to_string(),
                properties_json: serde_json::json!({}),
            }]);

        let selected = conversation_action_message_for_open(
            &snapshot,
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            object_id,
        );

        assert_eq!(
            selected.map(|message| message.action.subject),
            Some("Conversation Action".to_string())
        );
    }

    #[test]
    fn common_views_open_projects_default_navigation_shortcut() {
        let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
        let selected = navigation_shortcut_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            COMMON_VIEWS_FOLDER_ID,
            shortcut_id,
        );

        assert_eq!(
            selected.map(|message| message.subject),
            Some("Inbox".to_string())
        );
    }

    #[test]
    fn common_views_open_rejects_default_navigation_shortcut_from_wrong_folder() {
        let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
        let selected = navigation_shortcut_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            INBOX_FOLDER_ID,
            shortcut_id,
        );

        assert!(selected.is_none());
    }

    #[test]
    fn common_views_open_rejects_default_named_view_from_wrong_folder() {
        let view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
        let selected = common_view_named_view_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            INBOX_FOLDER_ID,
            view_id,
        );

        assert!(selected.is_none());
    }

    #[test]
    fn folder_default_named_view_open_materializes_for_target_folder() {
        let selected = common_view_named_view_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            INBOX_FOLDER_ID,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        );

        assert_eq!(
            selected.map(|message| (message.folder_id, message.name)),
            Some((INBOX_FOLDER_ID, "Messages".to_string()))
        );
    }

    #[test]
    fn folder_default_named_view_open_materializes_for_supported_contact_folder() {
        let selected = common_view_named_view_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            CONTACTS_FOLDER_ID,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        );

        assert_eq!(
            selected.map(|message| (message.folder_id, message.name)),
            Some((CONTACTS_FOLDER_ID, "Contacts".to_string()))
        );
    }

    #[test]
    fn conversation_action_open_rejects_default_action_from_wrong_folder() {
        let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
        let selected = conversation_action_message_for_open(
            &MapiMailStoreSnapshot::empty(),
            COMMON_VIEWS_FOLDER_ID,
            action_id,
        );

        assert!(selected.is_none());
    }

    #[test]
    fn virtual_default_conversation_action_set_properties_stages_pending_row() {
        let mut session = test_mapi_session();
        let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
        session.handles.insert(
            1,
            MapiObject::ConversationAction {
                folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                conversation_action_id: action_id,
            },
        );
        let handle_slots = vec![1];
        let request = RopRequest {
            rop_id: RopId::SetProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let result = stage_virtual_conversation_action_property_values(
            &mut session,
            &handle_slots,
            &request,
            &MapiMailStoreSnapshot::empty(),
            vec![(
                PID_TAG_SUBJECT_W,
                MapiValue::String("Conversation Action Update".to_string()),
            )],
        );

        assert!(matches!(result, Some(Ok(()))));
        match session.handles.get(&1) {
            Some(MapiObject::PendingConversationAction { properties, .. }) => {
                assert_eq!(
                    properties.get(&PID_TAG_SUBJECT_W),
                    Some(&MapiValue::String("Conversation Action Update".to_string()))
                );
            }
            other => panic!("expected pending conversation action, got {other:?}"),
        }
    }

    #[test]
    fn virtual_default_conversation_action_set_rejects_wrong_folder() {
        let mut session = test_mapi_session();
        let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
        session.handles.insert(
            1,
            MapiObject::ConversationAction {
                folder_id: COMMON_VIEWS_FOLDER_ID,
                conversation_action_id: action_id,
            },
        );
        let handle_slots = vec![1];
        let request = RopRequest {
            rop_id: RopId::SetProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let result = stage_virtual_conversation_action_property_values(
            &mut session,
            &handle_slots,
            &request,
            &MapiMailStoreSnapshot::empty(),
            vec![(
                PID_TAG_SUBJECT_W,
                MapiValue::String("Conversation Action Update".to_string()),
            )],
        );

        assert!(matches!(result, Some(Err(_))));
        assert!(matches!(
            session.handles.get(&1),
            Some(MapiObject::ConversationAction { .. })
        ));
    }

    #[test]
    fn virtual_default_conversation_action_delete_properties_stages_pending_row() {
        let mut session = test_mapi_session();
        let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
        session.handles.insert(
            1,
            MapiObject::ConversationAction {
                folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                conversation_action_id: action_id,
            },
        );
        let handle_slots = vec![1];
        let request = RopRequest {
            rop_id: RopId::DeleteProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let result = stage_virtual_conversation_action_property_delete(
            &mut session,
            &handle_slots,
            &request,
            &MapiMailStoreSnapshot::empty(),
            &[PID_TAG_SUBJECT_W],
        );

        assert!(matches!(result, Some(Ok(()))));
        match session.handles.get(&1) {
            Some(MapiObject::PendingConversationAction { properties, .. }) => {
                assert!(!properties.contains_key(&PID_TAG_SUBJECT_W));
            }
            other => panic!("expected pending conversation action, got {other:?}"),
        }
    }

    #[test]
    fn release_only_execute_response_echoes_input_handle_table() {
        let response_handles = execute_response_handle_table(&[], &[u32::MAX], &[], true);

        assert_eq!(response_handles, vec![u32::MAX]);
    }

    #[test]
    fn mixed_release_execute_response_preserves_sparse_output_handle_index() {
        let response_handles = execute_response_handle_table(
            &[0x02, 0x01, 0, 0, 0, 0, 0, 0],
            &[u32::MAX, 77],
            &[77],
            true,
        );

        assert_eq!(response_handles, vec![u32::MAX, 77]);
    }

    #[test]
    fn private_create_folder_response_never_sets_existing_folder_flag() {
        assert!(!private_create_folder_is_existing_response_flag());
    }

    #[test]
    fn deleted_advertised_quick_step_create_can_reuse_existing_real_folder() {
        let mut session = test_mapi_session();

        assert!(
            !create_folder_existing_mailbox_satisfies_deleted_advertised_request(
                &session,
                IPM_SUBTREE_FOLDER_ID,
                "Quick Step Settings",
            )
        );

        session.record_deleted_advertised_special_folder(QUICK_STEP_SETTINGS_FOLDER_ID);

        assert!(
            create_folder_existing_mailbox_satisfies_deleted_advertised_request(
                &session,
                IPM_SUBTREE_FOLDER_ID,
                "Quick Step Settings",
            )
        );
        assert!(
            !create_folder_existing_mailbox_satisfies_deleted_advertised_request(
                &session,
                IPM_SUBTREE_FOLDER_ID,
                "Ordinary Folder",
            )
        );
    }

    #[test]
    fn advertised_contact_folders_use_noop_delete_acknowledgement() {
        for folder_id in [
            CONTACTS_FOLDER_ID,
            SUGGESTED_CONTACTS_FOLDER_ID,
            QUICK_CONTACTS_FOLDER_ID,
            IM_CONTACT_LIST_FOLDER_ID,
        ] {
            assert!(
                is_advertised_special_folder(folder_id),
                "expected advertised special folder {folder_id:#018x}"
            );
            assert!(
                !advertised_special_folder_delete_uses_session_tombstone(folder_id),
                "contact folder delete must not hide the folder in session {folder_id:#018x}"
            );
            assert!(
                advertised_special_folder_delete_is_noop(folder_id),
                "contact folder delete should be acknowledged as non-destructive no-op {folder_id:#018x}"
            );
        }
        assert!(!advertised_special_folder_delete_is_noop(
            QUICK_STEP_SETTINGS_FOLDER_ID
        ));
        assert!(advertised_special_folder_delete_uses_session_tombstone(
            QUICK_STEP_SETTINGS_FOLDER_ID
        ));
    }

    #[test]
    fn uploaded_state_delta_anchor_requires_idset_and_cnset_seen() {
        let idset_only = upload_state_marker_bit(0x4017_0003);
        assert!(!uploaded_state_has_delta_anchor(idset_only));

        let cnset_only = upload_state_marker_bit(0x6796_0102);
        assert!(!uploaded_state_has_delta_anchor(cnset_only));

        assert!(uploaded_state_has_delta_anchor(idset_only | cnset_only));
    }

    #[test]
    fn uploaded_state_empty_stream_does_not_create_delta_anchor() {
        let mut marker_mask = 0;
        let uploaded_bytes = 0usize;

        if uploaded_bytes > 0 {
            mark_uploaded_state_stream(&mut marker_mask, 0x4017_0003);
            mark_uploaded_state_stream(&mut marker_mask, 0x6796_0102);
        }

        assert!(!uploaded_state_has_delta_anchor(marker_mask));
    }

    #[test]
    fn associated_config_stream_write_summary_names_roaming_xml() {
        let values = vec![
            (PID_TAG_ROAMING_DATATYPES, MapiValue::I32(2)),
            (
                PID_TAG_ROAMING_XML_STREAM,
                MapiValue::Binary(b"<xml/>".to_vec()),
            ),
            (0x685D_0003, MapiValue::I32(42)),
        ];

        let summary = associated_config_stream_write_summary(&values);

        assert!(summary.contains("PidTagRoamingDatatypes=i32"));
        assert!(summary.contains("PidTagRoamingXmlStream=binary:bytes=6"));
        assert!(summary.contains("OutlookConfigurationStamp=i32"));
    }

    #[test]
    fn empty_inbox_message_list_settings_save_gets_persistable_stream_defaults() {
        let properties = HashMap::from([
            (
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String("IPM.Configuration.MessageListSettings".to_string()),
            ),
            (PID_TAG_ROAMING_DATATYPES, MapiValue::I32(0)),
        ]);

        assert!(is_empty_inbox_message_list_settings_placeholder(
            INBOX_FOLDER_ID,
            "IPM.Configuration.MessageListSettings",
            &properties
        ));

        let with_payload = HashMap::from([
            (
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String("IPM.Configuration.MessageListSettings".to_string()),
            ),
            (
                PID_TAG_ROAMING_DICTIONARY,
                MapiValue::Binary(b"<xml/>".to_vec()),
            ),
        ]);
        assert!(!is_empty_inbox_message_list_settings_placeholder(
            INBOX_FOLDER_ID,
            "IPM.Configuration.MessageListSettings",
            &with_payload
        ));

        let default = crate::mapi_store::outlook_inbox_message_list_settings_default();
        let persisted = message_list_settings_placeholder_persisted_properties(&default);
        assert_eq!(
            persisted
                .get(&PID_TAG_ROAMING_DATATYPES)
                .cloned()
                .and_then(MapiValue::into_u32),
            Some(0x0000_0004)
        );
        assert!(matches!(
            persisted.get(&PID_TAG_ROAMING_DICTIONARY),
            Some(MapiValue::Binary(bytes)) if !bytes.is_empty()
        ));
        assert!(!is_empty_inbox_message_list_settings_placeholder(
            INBOX_FOLDER_ID,
            "IPM.Configuration.MessageListSettings",
            &persisted
        ));
    }

    #[test]
    fn associated_config_mutation_uses_saved_handle_when_snapshot_misses_row() {
        let config_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 219,
        );
        let saved = crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000219),
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "IPM.Configuration.MessageListSettings".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        };

        let resolved = associated_config_message_for_mutation(
            &MapiMailStoreSnapshot::empty(),
            INBOX_FOLDER_ID,
            config_id,
            Some(&saved),
        )
        .expect("saved handle fallback");

        assert_eq!(resolved.canonical_id, saved.canonical_id);
        assert_eq!(
            resolved.message_class,
            "IPM.Configuration.MessageListSettings"
        );
    }

    #[test]
    fn calendar_configuration_debug_contract_uses_roaming_properties() {
        let object = mapi_mailstore::SpecialMessageSyncFact {
            folder_id: CALENDAR_FOLDER_ID,
            item_id: 1,
            canonical_id: Uuid::nil(),
            associated: true,
            subject: "Calendar".to_string(),
            body_text: String::new(),
            message_class: "IPM.Configuration.Calendar".to_string(),
            last_modified_filetime: 0,
            message_size: 0,
            read_state: None,
            named_properties: vec![
                (
                    PID_TAG_ROAMING_DATATYPES,
                    mapi_mailstore::SpecialMessagePropertyValue::U32(4),
                ),
                (
                    PID_TAG_ROAMING_DICTIONARY,
                    mapi_mailstore::SpecialMessagePropertyValue::Binary(Vec::new()),
                ),
            ],
        };

        assert!(is_calendar_configuration_object(&object));
        let required_tags = format_calendar_required_property_tags(true, false);

        assert!(required_tags.contains("0x7c060003"));
        assert!(required_tags.contains("0x7c070102"));
        assert!(required_tags.contains("0x7c080102"));
        assert!(!required_tags.contains("0x00600040"));
        assert!(!required_tags.contains("0x820d0102"));
    }

    #[test]
    fn inbox_associated_config_summary_suppresses_virtual_only_rows() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000099999);
        let header_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 999,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            header_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 998,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: header_id,
                account_id,
                subject: "Mail".to_string(),
                target_folder_id: None,
                shortcut_type: 4,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
        ]);

        let summary = format_inbox_associated_config_summary(INBOX_FOLDER_ID, true, &snapshot);

        assert!(
            !summary.contains("class=IPM.Configuration.AccountPrefs"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Configuration.MessageListSettings"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Configuration.UMOLK.UserOptions"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Configuration.EAS"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Configuration.ELC"),
            "{summary}"
        );
        assert!(
            !summary.contains("class=IPM.Sharing.Configuration"),
            "{summary}"
        );
        assert!(!summary.contains("class=IPM.Sharing.Index"), "{summary}");
        assert!(!summary.contains("truncated="), "{summary}");
    }

    #[test]
    fn ipm_configuration_contract_summary_reports_required_columns_and_streams() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let canonical_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            canonical_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 121,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: canonical_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.MessageListSettings".to_string(),
                subject: "Message list settings".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
        ]);
        let columns = [
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_ROAMING_DATATYPES,
        ];
        let sort_orders = [
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 0,
            },
        ];

        let summary = format_ipm_configuration_contract_summary(
            INBOX_FOLDER_ID,
            true,
            &columns,
            &sort_orders,
            &snapshot,
        );

        assert!(summary.contains("not_selected_required_columns="));
        assert!(summary.contains("sort_by_message_class_then_lastmod=true"));
        assert!(summary.contains("row_issue_count=0"));
        assert!(summary.contains("datatypes=0x00000004"));
        assert!(summary.contains("has_dict=true"));
        assert!(summary.contains("associated_config_0e0b=binary:bytes=0"));
    }

    #[test]
    fn associated_config_wire_summary_uses_requested_position() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let first_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000011);
        let second_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000012);
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(0x7011),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(0x7012),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: first_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Custom.A".to_string(),
                subject: "A".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: second_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Custom.B".to_string(),
                subject: "B".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
        ]);

        let summary = format_inbox_associated_wire_row_summary(
            account_id,
            INBOX_FOLDER_ID,
            true,
            1,
            true,
            1,
            &[],
            None,
            &[PID_TAG_MESSAGE_CLASS_W],
            &snapshot,
        );

        assert!(summary.contains("position=1"), "{summary}");
        assert!(summary.contains("class=IPM.Custom.B"), "{summary}");
        assert!(!summary.contains("class=IPM.Custom.A"), "{summary}");
    }

    #[test]
    fn associated_config_debug_summaries_honor_table_restriction() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let eas_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000021);
        let umolk_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000022);
        crate::mapi::identity::remember_mapi_identity(
            eas_id,
            crate::mapi::identity::mapi_store_id(0x7021),
        );
        crate::mapi::identity::remember_mapi_identity(
            umolk_id,
            crate::mapi::identity::mapi_store_id(0x7022),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: eas_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.EAS".to_string(),
                subject: "IPM.Configuration.EAS".to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: umolk_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.UMOLK.UserOptions".to_string(),
                subject: "IPM.Configuration.UMOLK.UserOptions".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);
        let restriction = MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string()),
        };

        let window = format_inbox_associated_query_row_window(
            account_id,
            0,
            true,
            2,
            &[],
            Some(&restriction),
            &snapshot,
        );
        let values = format_outlook_query_row_values(
            account_id,
            INBOX_FOLDER_ID,
            true,
            0,
            true,
            2,
            &[],
            Some(&restriction),
            &[PID_TAG_MESSAGE_CLASS_W],
            &snapshot,
        );
        let wire = format_inbox_associated_wire_row_summary(
            account_id,
            INBOX_FOLDER_ID,
            true,
            0,
            true,
            2,
            &[],
            Some(&restriction),
            &[PID_TAG_MESSAGE_CLASS_W],
            &snapshot,
        );

        assert!(window.contains("total=1"), "{window}");
        assert!(
            window.contains("IPM.Configuration.UMOLK.UserOptions"),
            "{window}"
        );
        assert!(!window.contains("IPM.Configuration.EAS"), "{window}");
        assert!(
            values.contains("IPM.Configuration.UMOLK.UserOptions"),
            "{values}"
        );
        assert!(!values.contains("IPM.Configuration.EAS"), "{values}");
        assert!(
            wire.contains("IPM.Configuration.UMOLK.UserOptions"),
            "{wire}"
        );
        assert!(!wire.contains("IPM.Configuration.EAS"), "{wire}");
    }

    #[test]
    fn inbox_associated_named_view_debug_summaries_expose_folder_default_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty();
        let restriction = MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Microsoft.FolderDesign.NamedView".to_string()),
        };
        let columns = [
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
        ];

        let window = format_inbox_associated_query_row_window(
            account_id,
            0,
            true,
            1,
            &[],
            Some(&restriction),
            &snapshot,
        );
        let values = format_outlook_query_row_values(
            account_id,
            INBOX_FOLDER_ID,
            true,
            0,
            true,
            1,
            &[],
            Some(&restriction),
            &columns,
            &snapshot,
        );
        let wire = format_inbox_associated_wire_row_summary(
            account_id,
            INBOX_FOLDER_ID,
            true,
            0,
            true,
            1,
            &[],
            Some(&restriction),
            &columns,
            &snapshot,
        );

        assert!(window.contains("total=1"), "{window}");
        assert!(
            window.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
            "{window}"
        );
        assert!(
            values.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
            "{values}"
        );
        assert!(
            values.contains(&format!("0x67480014={INBOX_FOLDER_ID}")),
            "{values}"
        );
        assert!(
            values.contains(&format!(
                "0x674a0014={}",
                crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
            )),
            "{values}"
        );
        assert!(values.contains("0x683a0003=8"), "{values}");
        assert!(!wire.is_empty(), "{wire}");
        assert!(
            wire.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
            "{wire}"
        );
        assert!(wire.contains("value_len=32"), "{wire}");
        assert!(wire.contains("query_rows_len=33"), "{wire}");
    }

    #[test]
    fn common_views_query_row_values_report_selected_wlink_columns() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000001);
        let shortcut_store_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 131,
        );
        crate::mapi::identity::remember_mapi_identity(shortcut_id, shortcut_store_id);
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x10,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);

        let summary = format_outlook_query_row_values(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
            true,
            0,
            true,
            10,
            &[],
            None,
            &[
                PID_TAG_FOLDER_ID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_SUBJECT_W,
                PID_TAG_WLINK_ENTRY_ID,
                PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
                PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
            ],
            &snapshot,
        );

        assert!(summary.contains("index=0"));
        assert!(summary.contains(&format!("0x67480014={COMMON_VIEWS_FOLDER_ID}")));
        assert!(summary.contains(&format!("0x674d0014={shortcut_store_id}")));
        assert!(summary.contains("0x674e0003=0"));
        assert!(summary.contains("0x0037001f=Pinned Inbox"));
        assert!(summary.contains("0x684c0102=binary:"));
        assert!(summary.contains("0x68910102=binary:"));
        assert!(summary.contains("0x80100102=binary:"));
    }

    #[test]
    fn quick_step_associated_debug_summaries_report_custom_action_row() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty();
        let columns = [PID_TAG_MESSAGE_CLASS_W, PID_TAG_ROAMING_XML_STREAM];

        assert_eq!(
            effective_contents_table_columns(QUICK_STEP_SETTINGS_FOLDER_ID, true, &[]),
            default_associated_config_columns()
        );

        let values = format_outlook_query_row_values(
            account_id,
            QUICK_STEP_SETTINGS_FOLDER_ID,
            true,
            0,
            true,
            1,
            &[],
            None,
            &columns,
            &snapshot,
        );
        let wire = format_inbox_associated_wire_row_summary(
            account_id,
            QUICK_STEP_SETTINGS_FOLDER_ID,
            true,
            0,
            true,
            1,
            &[],
            None,
            &columns,
            &snapshot,
        );

        assert!(values.contains("IPM.Microsoft.CustomAction"), "{values}");
        assert!(values.contains("0x7c080102=binary:bytes="), "{values}");
        assert!(wire.contains("class=IPM.Microsoft.CustomAction"), "{wire}");
        assert!(wire.contains("query_rows_len="), "{wire}");
    }

    #[test]
    fn common_views_wlink_target_decoding_reports_inbox_match() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 131,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x10,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);

        let summary = format_common_views_wlink_target_decoding(account_id, &snapshot);

        assert!(summary.contains("subject=Pinned Inbox"));
        assert!(summary.contains(&format!("target_folder=0x{INBOX_FOLDER_ID:016x}")));
        assert!(summary.contains(&format!("entry_id_decoded=0x{INBOX_FOLDER_ID:016x}")));
        assert!(summary.contains("entry_id_matches_inbox=true"));
        assert!(summary.contains(&format!("source_key_decoded=0x{INBOX_FOLDER_ID:016x}")));
        assert!(summary.contains("source_key_matches_inbox=true"));
        assert!(summary.contains(&format!(
            "sharing_local_folder_id_decoded=0x{INBOX_FOLDER_ID:016x}"
        )));
        assert!(summary.contains("sharing_local_folder_id_matches_inbox=true"));
    }

    #[test]
    fn common_views_wlink_contract_distinguishes_expected_link_defaults() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000088888);
        let header_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 888,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            header_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 887,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: header_id,
                account_id,
                subject: "Mail".to_string(),
                target_folder_id: None,
                shortcut_type: 4,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
        ]);
        let columns = [
            PID_TAG_SUBJECT_W,
            PID_TAG_WLINK_ENTRY_ID,
            PID_TAG_WLINK_RECORD_KEY,
            PID_TAG_WLINK_STORE_ENTRY_ID,
            0x684f_0102,
            0x6850_0102,
            PID_TAG_WLINK_GROUP_NAME_W,
            PID_TAG_WLINK_SECTION,
            PID_TAG_WLINK_ORDINAL,
            PID_TAG_WLINK_TYPE,
            PID_TAG_WLINK_FLAGS,
            PID_TAG_WLINK_SAVE_STAMP,
            0x6842_0102,
            PID_TAG_WLINK_CALENDAR_COLOR,
            PID_TAG_WLINK_ADDRESS_BOOK_EID,
            PID_TAG_WLINK_CLIENT_ID,
            PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
            PID_TAG_WLINK_RO_GROUP_TYPE,
            0x6893_0102,
            PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
        ];

        let summary = format_common_views_wlink_contract_summary(&columns, &snapshot);

        assert!(summary.contains("link_rows=3"));
        assert!(summary.contains("header_rows=1"));
        assert!(summary.contains("not_selected_required_link_columns="));
        assert!(summary.contains("expected_link_default_columns=0x68530003"));
        assert!(!summary.contains("0x68420102"));
        assert!(summary.contains("0x68530003"));
        assert!(!summary.contains("0x68910102"));
        assert!(summary.contains("0x68930102"));
    }

    #[test]
    fn inbox_open_loop_summary_requires_repeated_probe_without_contents_table() {
        let mut state = PostHierarchyActionState::default();
        state.inbox_open_folder_probe_count = 2;
        state.inbox_folder_type_getprops_probe_count = 2;
        state
            .recent_probe_actions
            .push("Release(in=1,handle=2,kind=folder,folder=0x1)".to_string());

        let summary = format_inbox_open_loop_summary(&state).unwrap();

        assert!(summary.contains(&format!("folder=0x{INBOX_FOLDER_ID:016x}")));
        assert!(summary.contains("open_folder_count=2"));
        assert!(summary.contains("folder_type_getprops_count=2"));
        assert!(summary.contains("normal_contents_table_observed=false"));
        assert!(summary.contains("next_debug_focus=inbox_open_folder_loop"));
        assert!(summary.contains("last_common_views_inbox_shortcut=none"));
        assert!(summary.contains("last_inbox_hierarchy_table=none"));
        assert!(summary.contains("last_inbox_hierarchy_query=none"));
        assert!(summary.contains("last_inbox_related_release=none"));
        assert!(summary.contains("recent_actions=Release("));

        state.inbox_associated_contents_table_observed = true;
        let summary = format_inbox_open_loop_summary(&state).unwrap();
        assert!(summary.contains("next_debug_focus=common_views_or_inbox_fai_handoff"));
        state.last_inbox_hierarchy_query_context =
            "input_index=0;row_count=0;expected_subfolders=false".to_string();
        let summary = format_inbox_open_loop_summary(&state).unwrap();
        assert!(summary.contains("next_debug_focus=inbox_hierarchy_handoff"));

        state.inbox_normal_contents_table_observed = true;
        assert_eq!(format_inbox_open_loop_summary(&state), None);
    }

    #[test]
    fn inbox_post_fai_handoff_context_points_to_missing_contents_step() {
        let mut state = PostHierarchyActionState::default();
        state.inbox_associated_contents_table_observed = true;
        state.last_inbox_associated_query_context = "values=row0".to_string();
        state.last_common_views_inbox_shortcut_context = "entry_id_matches_inbox=true".to_string();
        state
            .recent_probe_actions
            .push("Release(in=0,handle=17,kind=contents_table,folder=0x5)".to_string());

        let context = format_inbox_post_fai_handoff_context(&state);

        assert!(context.contains("associated_contents_table_observed=true"));
        assert!(context.contains("normal_contents_table_observed=false"));
        assert!(context.contains("last_associated_query=values=row0"));
        assert!(context.contains("last_common_views_inbox_shortcut=entry_id_matches_inbox=true"));
        assert!(context.contains(
            "next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"
        ));
    }

    #[test]
    fn post_fai_hierarchy_release_context_reports_stop_before_inbox_contents() {
        let mut state = PostHierarchyActionState::default();
        state.inbox_associated_contents_table_observed = true;
        state.post_inbox_fai_handoff_logged = true;
        state.last_inbox_associated_query_context = "window=returned=6".to_string();
        state
            .recent_probe_actions
            .push("GetHierarchyTable(in=0,out=13,row_count=22)".to_string());
        let table = MapiObject::HierarchyTable {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            columns: vec![
                PID_TAG_FOLDER_ID,
                PID_TAG_SUBFOLDERS,
                PID_TAG_CONTAINER_CLASS_W,
            ],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 22,
        };

        let context = format_post_fai_hierarchy_release_without_inbox_contents_context(
            Some(&table),
            Some(13),
            &state,
            &[],
            &MapiMailStoreSnapshot::empty(),
        )
        .unwrap();

        assert!(context.contains("handle=13"));
        assert!(context.contains(&format!("folder=0x{IPM_SUBTREE_FOLDER_ID:016x}")));
        assert!(context.contains("role=ipm_subtree"));
        assert!(context.contains("row_count=22"));
        assert!(context.contains("last_associated_query=window=returned=6"));
        assert!(context.contains(
            "next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"
        ));
    }

    #[test]
    fn post_fai_folder_type_probe_loop_context_requires_reopen_and_repeated_probes() {
        let mut state = PostHierarchyActionState::default();
        state.post_inbox_fai_handoff_logged = true;
        state.post_inbox_fai_reopen_logged = true;
        state.inbox_associated_contents_table_observed = true;
        state.inbox_open_folder_probe_count = 3;
        state.inbox_folder_type_getprops_probe_count = 2;
        state.last_inbox_open_folder_context = "output_handle=25".to_string();
        state.last_inbox_folder_type_getprops_context = "folder_type=1".to_string();
        state.last_inbox_associated_query_context = "window=returned=6".to_string();
        state.last_inbox_related_release_context = "handle=20;role=ipm_subtree".to_string();
        state
            .recent_probe_actions
            .push("OpenFolder(in=1,handle=8,out=25,folder=0x0000000000050001)".to_string());
        state
            .recent_probe_actions
            .push("GetPropertiesSpecific(in=2,handle=25,tags=0x36010003)".to_string());

        let context = format_post_fai_folder_type_probe_loop_context(&state).unwrap();

        assert!(context.contains("open_folder_count=3"));
        assert!(context.contains("folder_type_getprops_count=2"));
        assert!(context.contains("last_open=output_handle=25"));
        assert!(context.contains("last_folder_type_getprops=folder_type=1"));
        assert!(context.contains("last_associated_query=window=returned=6"));
        assert!(context.contains("last_inbox_related_release=handle=20;role=ipm_subtree"));
        assert!(context.contains(
            "next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"
        ));

        state.inbox_normal_contents_table_observed = true;
        assert!(format_post_fai_folder_type_probe_loop_context(&state).is_none());
    }

    #[test]
    fn inbox_release_context_flags_visible_table_setcolumns_without_query_rows() {
        let mut state = PostHierarchyActionState::default();
        state.inbox_normal_contents_table_observed = true;
        state.inbox_normal_contents_table_setcolumns_observed = true;
        state.last_inbox_normal_contents_table_setcolumns_handle = Some(17);
        state.last_inbox_normal_contents_table_setcolumns_context =
            "handle=17;columns=0x67480014,0x674a0014,0x0037001f".to_string();
        let table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_FOLDER_ID, PID_TAG_MID, PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        let context = format_inbox_related_release_context(
            Some(&table),
            Some(17),
            &state,
            &MapiMailStoreSnapshot::empty(),
        )
        .unwrap();

        assert!(context.contains("associated=false"));
        assert!(context.contains("normal_setcolumns_observed=true"));
        assert!(context.contains("normal_query_rows_observed=false"));
        assert!(context.contains("visible_inbox_release_without_query_rows=true"));
        assert!(context.contains("last_normal_setcolumns_handle=17"));
        assert!(context.contains("last_normal_query_rows_handle=none"));
    }

    #[test]
    fn normal_message_column_support_covers_visible_inbox_probe_columns() {
        let summary = normal_message_table_column_support_summary(&[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
        ]);

        assert!(summary
            .contains("backed=0x67480014,0x674a0014,0x674d0014,0x674e0003,0x0037001f,0x0e060040"));
        assert!(summary.ends_with("defaulted=;named_or_dynamic="));
    }

    #[test]
    fn normal_message_column_support_covers_outlook_mail_view_columns() {
        for view_name in ["Messages", "Compact", "Sent To"] {
            let columns = outlook_mail_view_definition(view_name)
                .columns
                .iter()
                .map(|column| column.property_tag)
                .collect::<Vec<_>>();
            let summary = normal_message_table_column_support_summary(&columns);

            assert!(
                summary.contains(";defaulted=;"),
                "{view_name} view has defaulted message-table columns: {summary}"
            );
            assert!(
                summary.ends_with(match view_name {
                    "Compact" | "Messages" | "Sent To" => "named_or_dynamic=",
                    _ => unreachable!(),
                }),
                "{view_name} view has unexpected named/dynamic columns: {summary}"
            );
        }
    }

    #[test]
    fn normal_message_column_support_covers_observed_inbox_compact_projection() {
        let summary = normal_message_table_column_support_summary(&[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_CREATION_TIME,
            PID_TAG_SUBJECT_W,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_INTERNET_MESSAGE_ID_W,
            PID_TAG_IMPORTANCE,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_MESSAGE_STATUS,
            0x8514_000B,
            0x8017_000B,
            0x801F_001F,
            PID_TAG_SENT_REPRESENTING_ENTRY_ID,
            0x1213_0003,
            PID_TAG_MESSAGE_DELIVERY_TIME,
        ]);

        assert!(summary.contains("0x00410102"));
        assert!(!summary.contains("defaulted=0x00410102"));
        assert!(!summary.contains("defaulted=0x12130003"));
        assert!(summary.contains("0x8514000b"));
        assert!(summary.contains("0x8017000b"));
        assert!(summary.contains("0x801f001f"));
        assert!(summary.ends_with("named_or_dynamic="));
    }

    #[test]
    fn normal_message_column_support_backs_outlook_auxiliary_flags() {
        let detail =
            normal_message_defaulted_column_detail(&[PID_TAG_SUBJECT_W, 0x1213_0003, 0x801f_001f]);

        assert!(!detail.contains("tag=0x12130003"));
        assert!(!detail.contains("0x0037001f"));
        assert!(!detail.contains("0x801f001f"));
    }

    #[test]
    fn calendar_query_position_summary_projects_observed_outlook_columns() {
        let event_id = uuid::Uuid::from_u128(0x7174);
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(0x7174),
        );
        let event = lpe_storage::AccessibleEvent {
            id: event_id,
            uid: "calendar-row".to_string(),
            collection_id: DEFAULT_CALENDAR_COLLECTION_ID.to_string(),
            owner_account_id: uuid::Uuid::from_u128(0x8184),
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test User".to_string(),
            rights: default_mapping_rights(),
            date: "2026-06-23".to_string(),
            time: "15:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar row".to_string(),
            location: "Office".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![event],
            Vec::new(),
            Vec::new(),
        );

        let summary = format_calendar_event_query_position_summary(
            CALENDAR_FOLDER_ID,
            false,
            0,
            1,
            &[],
            None,
            &[
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_SUBJECT_W,
                PID_TAG_MESSAGE_FLAGS,
                PID_TAG_MESSAGE_STATUS,
                PID_LID_OUTLOOK_COMMON_8578_TAG,
                PID_LID_SIDE_EFFECTS_TAG,
            ],
            &snapshot,
        );

        assert!(summary.contains("event_total=1"));
        assert!(summary.contains("title=Calendar row"));
        assert!(summary.contains("0x85780003=0"));
        assert!(summary.contains("0x85100003=353"));
        assert!(!summary.contains("0x67480014=default"));
        assert!(!summary.contains("0x674d0014=default"));
        assert!(!summary.contains("0x674e0003=default"));
        assert!(!summary.contains("0x001a001f=default"));
        assert!(!summary.contains("0x0e170003=default"));
    }

    #[test]
    fn associated_column_support_covers_inbox_view_descriptor_columns() {
        let summary = associated_contents_table_column_support_summary(&[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_SUBJECT_W,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_CLSID,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            0x6842_0102,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_MESSAGE_CLASS_W,
        ]);

        assert!(summary.contains("0x68340003"));
        assert!(summary.contains("0x68330048"));
        assert!(summary.contains("0x683a0003"));
        assert!(summary.contains("0x68410003"));
        assert!(summary.contains("0x68420102"));
        assert!(summary.ends_with("defaulted=;named_or_dynamic="));
    }

    #[test]
    fn associated_column_support_covers_inbox_configuration_columns() {
        let summary = associated_contents_table_column_support_summary(&[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_MESSAGE_CLASS_W,
            0x685D_0003,
            PID_TAG_LAST_MODIFICATION_TIME,
        ]);

        assert!(summary.contains("0x7c060003"));
        assert!(summary.contains("0x685d0003"));
        assert!(summary.ends_with("defaulted=;named_or_dynamic="));
    }

    #[test]
    fn associated_column_support_covers_common_views_wlink_binary_variants() {
        let summary = associated_contents_table_column_support_summary(&[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_MESSAGE_CLASS_W,
            0x6842_0102,
            PID_TAG_WLINK_SAVE_STAMP,
            PID_TAG_SUBJECT_W,
            PID_TAG_WLINK_TYPE,
            PID_TAG_WLINK_FLAGS,
            PID_TAG_WLINK_ORDINAL,
            PID_TAG_WLINK_ENTRY_ID,
            PID_TAG_WLINK_RECORD_KEY,
            PID_TAG_WLINK_CALENDAR_COLOR,
            PID_TAG_WLINK_STORE_ENTRY_ID,
            0x684F_0102,
            0x6850_0102,
            PID_TAG_WLINK_GROUP_NAME_W,
            PID_TAG_WLINK_SECTION,
            PID_TAG_WLINK_ADDRESS_BOOK_EID,
            PID_TAG_WLINK_CLIENT_ID,
            PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
            PID_TAG_WLINK_RO_GROUP_TYPE,
            0x6893_0102,
            0x8010_0102,
        ]);

        assert!(summary.contains("0x684f0102"));
        assert!(summary.contains("0x68500102"));
        assert!(summary.contains("named_or_dynamic=0x80100102"));
        assert!(!summary.contains("defaulted=0x684f0102"));
        assert!(!summary.contains("defaulted=0x68500102"));
    }

    #[test]
    fn inbox_post_fai_reopen_stall_requires_handoff_release_without_normal_contents() {
        let mut state = PostHierarchyActionState::default();
        state.post_inbox_fai_handoff_logged = true;
        state.inbox_associated_contents_table_observed = true;
        state.last_inbox_related_release_context =
            "handle=16;kind=contents_table;associated=true".to_string();

        assert!(inbox_post_fai_reopen_stall_observed(&state));

        state.inbox_normal_contents_table_observed = true;
        assert!(!inbox_post_fai_reopen_stall_observed(&state));

        state.inbox_normal_contents_table_observed = false;
        state.last_inbox_related_release_context.clear();
        assert!(!inbox_post_fai_reopen_stall_observed(&state));
    }

    #[test]
    fn inbox_folder_type_getprops_response_context_includes_wire_preview() {
        let context = format_inbox_folder_type_getprops_response_context(&[
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        ]);

        assert!(context.contains("response_bytes=11"));
        assert!(context.contains("return_value=0x00000000"));
        assert!(context.contains("row_bytes=5"));
        assert!(context.contains("row_preview=0001000000"));
    }

    #[test]
    fn getprops_contract_response_summary_includes_access_value() {
        let summary = getprops_contract_response_summary(
            &[PID_TAG_ACCESS],
            &[
                0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00,
            ],
        );

        assert_eq!(summary.result, "0x00000000");
        assert_eq!(summary.returned_tags, "0x0ff40003");
        assert_eq!(summary.value_shapes, "0x0ff40003:0x0000003f");
    }

    #[test]
    fn execute_rop_debug_summary_decodes_ids_and_return_codes() {
        let mut request_bytes = vec![0x02, 0, 0, 1];
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        assert_eq!(request_summary.ids, vec![0x02]);
        assert_eq!(request_summary.ids_csv, "0x02");
        assert_eq!(request_summary.names_csv, "OpenFolder");
        assert_eq!(request_summary.handle_count, 1);
        assert!(request_summary.parse_error.is_empty());

        let request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let response_buffer =
            rop_buffer_with_response(rop_open_folder_response(&request, false), &[42]);
        let response_summary =
            summarize_response_rop_buffer(&response_buffer, &request_summary.ids);

        assert_eq!(response_summary.ids_csv, "0x02");
        assert_eq!(response_summary.names_csv, "OpenFolder");
        assert_eq!(response_summary.results_csv, "0x02:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload() {
        let mut request_bytes = vec![
            RopId::FindRow.as_u8(),
            0,
            3,
            0,
            0,
            0,
            1,
            0,
            0,
            RopId::GetPropertiesSpecific.as_u8(),
            0,
            5,
        ];
        request_bytes.extend_from_slice(&0u16.to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        let mut responses = vec![RopId::FindRow.as_u8(), 3];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.push(1);
        responses.extend_from_slice(&[0, 1, 0, 0, 0, 0]);
        responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 0x0e, 0x1a, 0, 0, 0]);
        responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 5, 0, 0, 0, 0, 0]);
        for unit in "Subject\0".encode_utf16() {
            responses.extend_from_slice(&unit.to_le_bytes());
        }
        let response_buffer = rop_buffer_with_response(responses, &[0]);
        let response_summary = summarize_response_rop_buffer_with_expected_handles(
            &response_buffer,
            &request_summary.full_ids,
            &request_summary.full_response_handle_indexes,
        );

        assert_eq!(response_summary.ids_csv, "0x4f,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x4f:0x00000000,0x07:0x00000000"
        );
        assert!(response_summary.frames.contains("0x07@"));
        assert!(!response_summary.results_csv.contains("0x0000001a"));
    }

    #[test]
    fn execute_rop_debug_summary_uses_output_handle_for_open_folder_response() {
        let mut request_bytes = vec![RopId::OpenFolder.as_u8(), 0, 0, 1];
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        request_bytes.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 0, 1, 0, 0, 0, 0]);
        let request_buffer = rop_buffer_with_response(request_bytes, &[0, u32::MAX]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        let request = RopRequest {
            rop_id: RopId::OpenFolder.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let mut responses = rop_open_folder_response(&request, false);
        responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 1]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&0u16.to_le_bytes());
        let response_buffer = rop_buffer_with_response(responses, &[ROOT_FOLDER_ID as u32, 42]);
        let response_summary = summarize_response_rop_buffer_with_expected_handles(
            &response_buffer,
            &request_summary.full_ids,
            &request_summary.full_response_handle_indexes,
        );

        assert_eq!(response_summary.ids_csv, "0x02,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x02:0x00000000,0x07:0x00000000"
        );
    }

    #[test]
    fn execute_rop_debug_summary_uses_output_handle_for_open_stream_response() {
        let mut request_bytes = vec![RopId::OpenMessage.as_u8(), 0, 0, 1];
        request_bytes.extend_from_slice(&0u16.to_le_bytes());
        request_bytes.extend_from_slice(&ROOT_FOLDER_ID.to_le_bytes());
        request_bytes.push(0);
        request_bytes.extend_from_slice(&0x7fff_ffff_ffed_0001u64.to_le_bytes());
        request_bytes.extend_from_slice(&[RopId::OpenStream.as_u8(), 0, 1, 2]);
        request_bytes.extend_from_slice(&0x6802_0102u32.to_le_bytes());
        request_bytes.push(0);
        request_bytes.extend_from_slice(&[RopId::ReadStream.as_u8(), 0, 2]);
        request_bytes.extend_from_slice(&0xffffu16.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[0, u32::MAX, u32::MAX]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        let open_message_request = RopRequest {
            rop_id: RopId::OpenMessage.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let open_stream_request = RopRequest {
            rop_id: RopId::OpenStream.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: Some(2),
            payload: Vec::new(),
        };
        let mut responses =
            rop_open_message_response(&open_message_request, "IPM.RuleOrganizer", 0);
        responses.extend_from_slice(&rop_open_stream_response(&open_stream_request, 0));
        responses.extend_from_slice(&[RopId::ReadStream.as_u8(), 2]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&0u16.to_le_bytes());
        let response_buffer = rop_buffer_with_response(responses, &[ROOT_FOLDER_ID as u32, 42, 43]);
        let response_summary = summarize_response_rop_buffer_with_expected_handles(
            &response_buffer,
            &request_summary.full_ids,
            &request_summary.full_response_handle_indexes,
        );

        assert_eq!(response_summary.ids_csv, "0x03,0x2b,0x2c");
        assert_eq!(
            response_summary.results_csv,
            "0x03:0x00000000,0x2b:0x00000000,0x2c:0x00000000"
        );
    }

    #[test]
    fn execute_rop_debug_summary_distinguishes_truncated_release_prefix() {
        let mut request_bytes = Vec::new();
        for index in 0..MAX_ROP_DEBUG_ENTRIES {
            request_bytes.extend_from_slice(&[RopId::Release.as_u8(), 0, index as u8]);
        }
        request_bytes.extend_from_slice(&[RopId::OpenFolder.as_u8(), 0, 0, 1]);
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        let request_buffer =
            rop_buffer_with_response(request_bytes, &vec![u32::MAX; MAX_ROP_DEBUG_ENTRIES + 1]);

        let request_summary = summarize_request_rop_buffer(&request_buffer);

        assert_eq!(request_summary.full_ids.len(), MAX_ROP_DEBUG_ENTRIES + 1);
        assert_eq!(request_summary.ids.len(), MAX_ROP_DEBUG_ENTRIES);
        assert_eq!(request_summary.total_count, MAX_ROP_DEBUG_ENTRIES + 1);
        assert!(request_summary.truncated);
        assert!(!request_summary.all_release);
        assert!(request_summary.ids.iter().all(|rop_id| *rop_id == 0x01));
        assert!(request_summary.tail_ids_csv.ends_with("0x02"));
        assert!(request_summary.tail_names_csv.ends_with("OpenFolder"));
        assert_eq!(
            request_summary.non_release_rops,
            format!("{}:OpenFolder", MAX_ROP_DEBUG_ENTRIES)
        );
        assert!(request_summary.parse_error.is_empty());
    }

    #[test]
    fn post_sync_release_flags_counts_outlook_close_handles() {
        let events = vec![
            PostHierarchyReleaseDebugEvent {
                input_handle_index: 0,
                handle: "1".to_string(),
                object_kind: "synchronization_source".to_string(),
                folder_id: "0x0000000000050001".to_string(),
                remaining_before: 4,
                remaining_after: 3,
                logon_before_content_sync: false,
            },
            PostHierarchyReleaseDebugEvent {
                input_handle_index: 1,
                handle: "2".to_string(),
                object_kind: "synchronization_collector".to_string(),
                folder_id: "0x0000000000050001".to_string(),
                remaining_before: 3,
                remaining_after: 2,
                logon_before_content_sync: false,
            },
            PostHierarchyReleaseDebugEvent {
                input_handle_index: 2,
                handle: "3".to_string(),
                object_kind: "notification_subscription".to_string(),
                folder_id: "none".to_string(),
                remaining_before: 2,
                remaining_after: 1,
                logon_before_content_sync: false,
            },
            PostHierarchyReleaseDebugEvent {
                input_handle_index: 3,
                handle: "4".to_string(),
                object_kind: "logon".to_string(),
                folder_id: "none".to_string(),
                remaining_before: 1,
                remaining_after: 0,
                logon_before_content_sync: false,
            },
        ];

        let flags = post_sync_release_flags(&events);

        assert!(flags.contains("logon=1"), "{flags}");
        assert!(flags.contains("synchronization_source=1"), "{flags}");
        assert!(flags.contains("synchronization_collector=1"), "{flags}");
        assert!(flags.contains("notification_subscription=1"), "{flags}");
        assert!(flags.contains("folder=0"), "{flags}");
    }

    #[test]
    fn execute_rop_response_summary_uses_full_truncated_request_ids() {
        let mut request_bytes = Vec::new();
        for index in 0..MAX_ROP_DEBUG_ENTRIES {
            request_bytes.extend_from_slice(&[RopId::Release.as_u8(), 0, index as u8]);
        }
        request_bytes.extend_from_slice(&[RopId::GetPropertyIdsFromNames.as_u8(), 0, 45, 0x02]);
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.push(0x00);
        request_bytes.extend_from_slice(&[0x02; 16]);
        request_bytes.extend_from_slice(&0x820du32.to_le_bytes());
        let request_buffer =
            rop_buffer_with_response(request_bytes, &vec![u32::MAX; MAX_ROP_DEBUG_ENTRIES + 1]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);
        let property_ids_request = RopRequest {
            rop_id: RopId::GetPropertyIdsFromNames.as_u8(),
            input_handle_index: Some(45),
            output_handle_index: Some(45),
            payload: Vec::new(),
        };
        let response_buffer = rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(
            rop_get_property_ids_from_names_response(&property_ids_request, &[0x820d]),
            &[0],
        ));

        let truncated_response =
            summarize_response_rop_buffer(&response_buffer, &request_summary.ids);
        let full_response =
            summarize_response_rop_buffer(&response_buffer, &request_summary.full_ids);

        assert_eq!(truncated_response.count, 0);
        assert_eq!(full_response.ids_csv, "0x56");
        assert_eq!(full_response.names_csv, "GetPropertyIdsFromNames");
        assert_eq!(full_response.results_csv, "0x56:0x00000000");
        assert!(full_response.parse_error.is_empty());
    }

    #[test]
    fn get_buffer_response_debug_exposes_wire_framing() {
        let mut response = vec![0x4e, 0x03];
        response.extend_from_slice(&0u32.to_le_bytes());
        response.extend_from_slice(&0x0003u16.to_le_bytes());
        response.extend_from_slice(&2u16.to_le_bytes());
        response.extend_from_slice(&2u16.to_le_bytes());
        response.push(0);
        response.extend_from_slice(&4u16.to_le_bytes());
        response.extend_from_slice(&[0x40, 0x12, 0x00, 0x03]);

        let debug = summarize_fast_transfer_get_buffer_response(&response, true);

        assert_eq!(debug.rop_id, "0x4e");
        assert!(debug.rop_id_matches);
        assert_eq!(debug.handle_index, 3);
        assert_eq!(debug.return_value, "0x00000000");
        assert_eq!(debug.transfer_status, "0x0003");
        assert!(debug.transfer_status_matches_completed);
        assert_eq!(debug.in_progress_count, 2);
        assert_eq!(debug.total_step_count, 2);
        assert!(debug.reserved_zero);
        assert_eq!(debug.transfer_buffer_size, 4);
        assert_eq!(debug.transfer_payload_bytes, 4);
        assert!(debug.transfer_buffer_size_matches_payload);
        assert_eq!(debug.transfer_payload_preview_hex, "40120003");
        assert!(debug.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_debug_summary_skips_release_rops_without_responses() {
        let request = RopRequest {
            rop_id: 0x7F,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: 2u32.to_le_bytes().to_vec(),
        };
        let response_buffer =
            rop_buffer_with_response(rop_get_local_replica_ids_response(&request, 42), &[42]);
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x01, 0x7F]);

        assert_eq!(response_summary.ids_csv, "0x7f");
        assert_eq!(response_summary.results_csv, "0x7f:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_keeps_get_address_types_frame_boundary() {
        let address_types_request = RopRequest {
            rop_id: 0x49,
            input_handle_index: Some(0),
            output_handle_index: Some(0),
            payload: Vec::new(),
        };
        let open_folder_request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(1),
            output_handle_index: Some(2),
            payload: Vec::new(),
        };
        let mut responses = rop_get_address_types_response(&address_types_request);
        responses.extend_from_slice(&rop_open_folder_response(&open_folder_request, false));
        responses.extend_from_slice(&[0x07, 0x02, 0, 0, 0, 0, 1, 1, 0, 0, 0]);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 5, 20]));
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x49, 0x02, 0x07]);

        assert_eq!(response_summary.ids_csv, "0x49,0x02,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x49:0x00000000,0x02:0x00000000,0x07:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x49@0..18:len=18:out=0:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x02@18..26:len=8:out=2:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x07@26..37:len=11:out=2:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_keeps_get_property_ids_frame_boundary() {
        let property_ids_request = RopRequest {
            rop_id: 0x56,
            input_handle_index: Some(0),
            output_handle_index: Some(0),
            payload: Vec::new(),
        };
        let open_folder_request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(1),
            output_handle_index: Some(2),
            payload: Vec::new(),
        };
        let mut responses =
            rop_get_property_ids_from_names_response(&property_ids_request, &[0x8003, 0x8004]);
        responses.extend_from_slice(&rop_open_folder_response(&open_folder_request, false));
        responses.extend_from_slice(&[0x07, 0x02, 0, 0, 0, 0, 1, 1, 0, 0, 0]);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[13, 4, 17]));
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x56, 0x02, 0x07]);

        assert_eq!(response_summary.ids_csv, "0x56,0x02,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x56:0x00000000,0x02:0x00000000,0x07:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x56@0..12:len=12:out=0:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x02@12..20:len=8:out=2:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x07@20..31:len=11:out=2:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_keeps_contents_table_frame_boundary() {
        let table_request = RopRequest {
            rop_id: 0x05,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x02],
        };
        let set_columns_request = RopRequest {
            rop_id: 0x12,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let sort_table_request = RopRequest {
            rop_id: 0x13,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let seek_row_response = vec![0x18, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut responses = rop_get_contents_table_response(&table_request, 0x12);
        responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));
        responses.extend_from_slice(&rop_sort_table_response(&sort_table_request));
        responses.extend_from_slice(&seek_row_response);
        responses.extend_from_slice(&rop_get_contents_table_response(&table_request, 13));

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[42, 43]));
        let response_summary =
            summarize_response_rop_buffer(&response_buffer, &[0x05, 0x12, 0x13, 0x18, 0x05]);

        assert_eq!(response_summary.ids_csv, "0x05,0x12,0x13,0x18,0x05");
        assert_eq!(
            response_summary.results_csv,
            "0x05:0x00000000,0x12:0x00000000,0x13:0x00000000,0x18:0x00000000,0x05:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x05@0..10:len=10:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x12@10..17:len=7:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x13@17..24:len=7:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x18@24..35:len=11:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x05@35..45:len=10:out=1:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_skips_implausible_query_rows_payload_marker() {
        let table_request = RopRequest {
            rop_id: 0x05,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x02],
        };
        let set_columns_request = RopRequest {
            rop_id: 0x12,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let sort_table_request = RopRequest {
            rop_id: 0x13,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let seek_row_response = vec![0x18, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut find_row_response = vec![0x4f, 0x01, 0, 0, 0, 0, 0, 1];
        find_row_response.extend_from_slice(&[0x01, 0x00, 0x15, 0x49, 0x00, 0x50, 0x00, 0x46]);
        let query_rows_response = vec![0x15, 0x03, 0, 0, 0, 0, 0, 0, 0];
        let mut responses = rop_get_contents_table_response(&table_request, 2);
        responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));
        responses.extend_from_slice(&rop_sort_table_response(&sort_table_request));
        responses.extend_from_slice(&seek_row_response);
        responses.extend_from_slice(&find_row_response);
        responses.extend_from_slice(&query_rows_response);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 2, 3, 4]));
        let response_summary =
            summarize_response_rop_buffer(&response_buffer, &[0x05, 0x12, 0x13, 0x18, 0x4f, 0x15]);

        assert_eq!(response_summary.ids_csv, "0x05,0x12,0x13,0x18,0x4f,0x15");
        assert_eq!(
            response_summary.results_csv,
            "0x05:0x00000000,0x12:0x00000000,0x13:0x00000000,0x18:0x00000000,0x4f:0x00000000,0x15:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x4f@35..51:len=16:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x15@51..60:len=9:out=3:rv=0x00000000"));
        assert!(!response_summary.results_csv.contains("0x15:0x46005000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_keeps_create_setprops_save_frame_boundary() {
        let mut responses = vec![0x06, 0x02];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&[0x29, 0x03]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&[0x0a, 0x04]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&0u16.to_le_bytes());
        responses.extend_from_slice(&[0x07, 0x04]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&1u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&[0x0a, 0x04]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&PID_TAG_NORMALIZED_SUBJECT_W.to_le_bytes());
        responses.extend_from_slice(&0x8004_0102u32.to_le_bytes());
        responses.extend_from_slice(&[0x0c, 0x05]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(4);
        responses.extend_from_slice(&0x0000_0000_0000_1234u64.to_le_bytes());

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 4, 5, 6]));
        let response_summary = summarize_response_rop_buffer(
            &response_buffer,
            &[0x01, 0x06, 0x29, 0x0a, 0x07, 0x0a, 0x0c],
        );

        assert_eq!(response_summary.ids_csv, "0x06,0x29,0x0a,0x07,0x0a,0x0c");
        assert_eq!(
            response_summary.results_csv,
            "0x06:0x00000000,0x29:0x00000000,0x0a:0x00000000,0x07:0x00000000,0x0a:0x00000000,0x0c:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x06@0..7:len=7:out=2:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x29@7..14:len=7:out=3:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x0a@14..22:len=8:out=4:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x07@22..33:len=11:out=4:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x0a@33..51:len=18:out=4:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x0c@51..66:len=15:out=5:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn save_changes_success_response_updates_response_handle_slot() {
        let request = RopRequest {
            rop_id: 0x0c,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0],
        };
        let mut responses = Vec::new();
        let mut handle_slots = vec![77, u32::MAX];

        append_save_changes_message_response(
            &mut responses,
            &mut handle_slots,
            &request,
            77,
            0x0000_0000_0000_1234,
        );

        assert_eq!(handle_slots, vec![77, 77]);
        assert_eq!(responses[0], 0x0c);
        assert_eq!(responses[1], 1);
    }

    #[test]
    fn execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop() {
        let table_request = RopRequest {
            rop_id: 0x05,
            input_handle_index: Some(0),
            output_handle_index: Some(2),
            payload: vec![0x02],
        };
        let set_columns_request = RopRequest {
            rop_id: 0x12,
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let mut responses = vec![0x4F, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.push(1);
        responses.extend_from_slice(&[
            0x00, 0x01, 0x00, 0x01, 0x05, 0x01, 0x00, 0x7f, 0xff, 0x00, 0x44, 0x55,
        ]);
        let find_row_end = responses.len();
        responses.extend_from_slice(&rop_get_contents_table_response(&table_request, 3));
        responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[42, 43]));
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x4F, 0x05, 0x12]);

        assert_eq!(response_summary.ids_csv, "0x4f,0x05,0x12");
        assert_eq!(
            response_summary.results_csv,
            "0x4f:0x00000000,0x05:0x00000000,0x12:0x00000000"
        );
        assert!(response_summary.frames.contains(&format!(
            "0x4f@0..{find_row_end}:len={find_row_end}:out=1:rv=0x00000000"
        )));
        assert!(!response_summary.results_csv.contains("0xffff7f00"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker() {
        let mut responses = vec![0x07, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835.to_le_bytes());
        responses.extend_from_slice(&8u32.to_le_bytes());
        responses.extend_from_slice(&[0x01, 0x02, 0x07, 0x74, 0x1f, 0x6f, 0xd3, 0x03]);
        let first_getprops_end = responses.len();
        responses.extend_from_slice(&[0x07, 0x01]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        responses.extend_from_slice(&0u16.to_le_bytes());

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[0x0000_0001]));
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x07, 0x07]);

        assert_eq!(response_summary.ids_csv, "0x07,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x07:0x00000000,0x07:0x00000000"
        );
        assert!(response_summary.frames.contains(&format!(
            "0x07@0..{first_getprops_end}:len={first_getprops_end}:out=1:rv=0x00000000"
        )));
        assert!(!response_summary.results_csv.contains("0xd36f1f74"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_summary_skips_bare_warning_getprops_payload_marker() {
        let mut responses = vec![0x4F, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.push(1);
        responses.extend_from_slice(&[0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x04, 0x00]);
        let find_row_end = responses.len();
        responses.extend_from_slice(&[0x07, 0x02]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[0x0000_0001]));
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x4F, 0x07]);

        assert_eq!(response_summary.ids_csv, "0x4f,0x07");
        assert_eq!(
            response_summary.results_csv,
            "0x4f:0x00000000,0x07:0x00000000"
        );
        assert!(response_summary.frames.contains(&format!(
            "0x4f@0..{find_row_end}:len={find_row_end}:out=1:rv=0x00000000"
        )));
        assert!(!response_summary.results_csv.contains("0x07:0x00040000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_framing_summary_marks_multi_rop_boundaries() {
        let mut responses = Vec::new();
        responses.push(0x02);
        responses.push(1);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.push(0);
        for rop_id in [0x70, 0x75, 0x77, 0x75, 0x77] {
            responses.push(rop_id);
            responses.push(1);
            responses.extend_from_slice(&0u32.to_le_bytes());
        }
        responses.push(0x4E);
        responses.push(2);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&0x0003u16.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&4u16.to_le_bytes());
        responses.extend_from_slice(&[0x03, 0x00, 0x14, 0x40]);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 4, 3]));
        let response_summary = summarize_response_rop_buffer(
            &response_buffer,
            &[0x02, 0x70, 0x75, 0x77, 0x75, 0x77, 0x4E],
        );

        assert_eq!(response_summary.buffer_layout, "rpc_header_ext_spec");
        assert_eq!(response_summary.response_payload_bytes, 57);
        assert_eq!(response_summary.handle_table_bytes, 12);
        assert_eq!(response_summary.count, 7);
        assert_eq!(
            response_summary.results_csv,
            "0x02:0x00000000,0x70:0x00000000,0x75:0x00000000,0x77:0x00000000,0x75:0x00000000,0x77:0x00000000,0x4e:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x02@0..8:len=8:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x4e@38..57:len=19:out=2:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_response_framing_context_includes_bootstrap_getprops_batches() {
        assert_eq!(
            execute_response_framing_context(&[0x07]),
            Some("getprops_or_release_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x07]),
            Some("getprops_or_release_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x01]),
            Some("release_only")
        );
        assert_eq!(
            execute_response_framing_context(&[0x02, 0x70, 0x4E]),
            Some("hierarchy_sync")
        );
        assert_eq!(
            execute_response_framing_context(&[0x49, 0x02, 0x07]),
            Some("named_props_openfolder_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x56, 0x02, 0x07]),
            Some("named_props_openfolder_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x05, 0x12, 0x13, 0x18, 0x4F]),
            Some("contents_table_probe")
        );
        assert_eq!(
            execute_response_framing_context(&[
                0x05, 0x12, 0x13, 0x18, 0x4F, 0x56, 0x05, 0x12, 0x13, 0x4F,
            ]),
            Some("contents_table_probe")
        );
        assert_eq!(
            execute_response_framing_context(&[
                0x05, 0x12, 0x13, 0x18, 0x4F, 0x56, 0x04, 0x12, 0x15, 0x29, 0x07, 0x14,
            ]),
            Some("contents_table_batch")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x02, 0x07]),
            Some("openfolder_getprops_probe")
        );
        assert_eq!(execute_response_framing_context(&[0x0A]), Some("setprops"));
        assert_eq!(execute_response_framing_context(&[0x79]), Some("setprops"));
        assert_eq!(
            execute_response_framing_context(&[0x03, 0x07, 0x01, 0x0a]),
            Some("open_message_getprops_setprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x06, 0x29, 0x0a, 0x07, 0x0a, 0x0c]),
            Some("create_message_setprops_save")
        );
        assert_eq!(
            execute_response_framing_context(&[0x02, 0x07]),
            Some("openfolder_getprops_probe")
        );
    }

    #[test]
    fn builtin_search_criteria_fallback_covers_advertised_reminders_folder() {
        let (restriction, folder_ids, flags) =
            builtin_search_criteria_to_rop_for_folder_id(REMINDERS_FOLDER_ID)
                .expect("reminders built-in search criteria");

        assert!(restriction.is_empty());
        assert_eq!(folder_ids, vec![CALENDAR_FOLDER_ID, TASKS_FOLDER_ID]);
        assert_eq!(flags, SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG);
        assert_eq!(
            builtin_search_role_for_folder_id(REMINDERS_FOLDER_ID),
            Some("reminders")
        );
    }

    #[test]
    fn builtin_search_criteria_fallback_covers_tracked_mail_processing_folder() {
        let (restriction, folder_ids, flags) =
            builtin_search_criteria_to_rop_for_folder_id(TRACKED_MAIL_PROCESSING_FOLDER_ID)
                .expect("tracked mail processing built-in search criteria");

        assert!(restriction.is_empty());
        assert_eq!(folder_ids, vec![IPM_SUBTREE_FOLDER_ID]);
        assert_eq!(flags, SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG);
        assert_eq!(
            builtin_search_role_for_folder_id(TRACKED_MAIL_PROCESSING_FOLDER_ID),
            Some("tracked_mail_processing")
        );
    }

    #[test]
    fn search_criteria_debug_scope_reports_invalid_folder_ids() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(&SEARCH_RUNNING_FLAG.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::SetSearchCriteria.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let context = format_debug_search_criteria_scope(&request);

        assert!(context.contains("parse=invalid_folder_id"));
        assert!(context.contains("folder_count=1"));
        assert!(context.contains("invalid:0000000000000000"));
        assert!(context.contains("flags=0x00000001"));
    }

    #[test]
    fn blank_search_criteria_is_invalid() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&INBOX_FOLDER_ID.to_le_bytes());
        payload.extend_from_slice(&(SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG).to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::SetSearchCriteria.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let error =
            bounded_search_criteria_from_rop(&request, INBOX_FOLDER_ID, None, &[]).unwrap_err();

        assert_eq!(error, EC_SEARCH_INVALID_PARAMETER);
    }

    #[test]
    fn event_19_candidate_detects_same_batch_save_getprops_not_found() {
        let request = RopRequestDebugSummary {
            ids: vec![
                0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x06, 0x0a, 0x0a, 0x0a, 0x0a, 0x0c,
                0x07,
            ],
            ..RopRequestDebugSummary::default()
        };
        let response = RopResponseDebugSummary {
            results_csv: "0x06:0x00000000,0x0a:0x00000000,0x0c:0x00000000,0x07:0x8004010f"
                .to_string(),
            ..RopResponseDebugSummary::default()
        };

        assert!(execute_batch_has_same_save_getprops_not_found(
            &request, &response
        ));

        let response = RopResponseDebugSummary {
            results_csv: "0x06:0x00000000,0x0a:0x00000000,0x0c:0x00000000,0x07:0x00000000"
                .to_string(),
            ..RopResponseDebugSummary::default()
        };

        assert!(!execute_batch_has_same_save_getprops_not_found(
            &request, &response
        ));
    }

    #[test]
    fn long_term_id_from_id_rejects_unparsed_or_not_loaded_scope() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 1,
        );
        let request = RopRequest {
            rop_id: RopId::LongTermIdFromId as u8,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: crate::mapi::identity::wire_id_bytes_from_object_id(object_id)
                .unwrap()
                .to_vec(),
        };

        assert_eq!(
            rop_long_term_id_from_id_response_for_scope(&request, None, "not_loaded"),
            vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
        assert_eq!(
            &rop_long_term_id_from_id_response_for_scope(&request, None, "message")[..6],
            &[RopId::LongTermIdFromId as u8, 0x00, 0, 0, 0, 0]
        );
        assert_eq!(
            rop_long_term_id_from_id_response_for_scope(&request, None, "unparsed"),
            vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
    }

    #[test]
    fn folder_set_property_problems_accepts_ipm_subtree_ostid_write() {
        let ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };

        assert!(folder_set_property_problems(
            Some(&ipm_subtree),
            &[],
            &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
        )
        .is_empty());
        assert_eq!(
            folder_set_property_problems(
                Some(&ipm_subtree),
                &[],
                &[(PID_TAG_OST_OSTID, MapiValue::Binary(Vec::new()))],
            ),
            vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&ipm_subtree),
                &[],
                &[(PID_TAG_DISPLAY_NAME_W, MapiValue::String("IPM".to_string()))],
            ),
            vec![(0, PID_TAG_DISPLAY_NAME_W, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&inbox),
                &[],
                &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
            ),
            vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
        );
    }

    #[test]
    fn folder_set_property_problems_accepts_additional_ren_entry_ids_ex_on_root_and_inbox() {
        let root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let value = MapiValue::Binary(vec![1; 490]);

        assert!(folder_set_property_problems(
            Some(&root),
            &[],
            &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
        )
        .is_empty());
        assert!(folder_set_property_problems(
            Some(&inbox),
            &[],
            &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
        )
        .is_empty());
        assert_eq!(
            folder_set_property_problems(
                Some(&ipm_subtree),
                &[],
                &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
            ),
            vec![(0, PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&root),
                &[],
                &[(
                    PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
                    MapiValue::Binary(Vec::new())
                )],
            ),
            vec![(0, PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, 0x8004_0102)]
        );
    }

    #[test]
    fn folder_set_property_problems_accepts_hidden_write_on_quick_step_folder() {
        let quick_step = JmapMailbox {
            id: Uuid::parse_str("f54d192a-3149-4ff1-bde7-a8dac219c73b").unwrap(),
            parent_id: None,
            role: "custom".to_string(),
            name: "Quick Step Settings".to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let quick_step_folder = MapiObject::Folder {
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let regular_folder = MapiObject::Folder {
            folder_id: 0x0000_0000_1234_0001,
            properties: std::collections::HashMap::new(),
        };

        assert!(folder_set_property_problems(
            Some(&quick_step_folder),
            std::slice::from_ref(&quick_step),
            &[(PID_TAG_ATTRIBUTE_HIDDEN, MapiValue::Bool(true))],
        )
        .is_empty());
        assert!(folder_set_property_problems(
            Some(&quick_step_folder),
            std::slice::from_ref(&quick_step),
            &[(
                PID_TAG_CONTAINER_CLASS_W,
                MapiValue::String("IPF.Configuration".to_string())
            )],
        )
        .is_empty());
        assert_eq!(
            folder_set_property_problems(
                Some(&quick_step_folder),
                std::slice::from_ref(&quick_step),
                &[(
                    PID_TAG_ATTRIBUTE_HIDDEN,
                    MapiValue::String("true".to_string())
                )],
            ),
            vec![(0, PID_TAG_ATTRIBUTE_HIDDEN, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&quick_step_folder),
                std::slice::from_ref(&quick_step),
                &[(
                    PID_TAG_CONTAINER_CLASS_W,
                    MapiValue::String("IPF.Note".to_string())
                )],
            ),
            vec![(0, PID_TAG_CONTAINER_CLASS_W, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&regular_folder),
                std::slice::from_ref(&quick_step),
                &[(PID_TAG_ATTRIBUTE_HIDDEN, MapiValue::Bool(true))],
            ),
            vec![(0, PID_TAG_ATTRIBUTE_HIDDEN, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&regular_folder),
                std::slice::from_ref(&quick_step),
                &[(
                    PID_TAG_CONTAINER_CLASS_W,
                    MapiValue::String("IPF.Configuration".to_string())
                )],
            ),
            vec![(0, PID_TAG_CONTAINER_CLASS_W, 0x8004_0102)]
        );
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex() {
        let Some(MapiValue::Binary(value)) = special_folder_identification_property_value(
            test_principal().account_id,
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        ) else {
            panic!("expected AdditionalRenEntryIdsEx");
        };

        let debug = default_folder_entry_id_values_for_debug(&[(
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
            MapiValue::Binary(value),
        )]);

        assert!(debug.contains("PidTagAdditionalRenEntryIdsEx:bytes="));
        assert!(debug.contains("bytes=544"));
        assert!(debug.contains("entry_count=10"));
        assert!(debug.contains("persist_id=0x8006"));
        assert!(debug.contains("persist_name=conversation_actions"));
        assert!(debug.contains("decoded_name=conversation_action_settings"));
        assert!(debug.contains("persist_id=0x8007"));
        assert!(debug.contains("persist_name=quick_step_settings"));
        assert!(debug.contains("decoded_name=quick_step_settings"));
        assert!(debug.contains("persist_id=0x800f"));
        assert!(debug.contains("persist_name=archive"));
        assert!(debug.contains("decoded_name=archive"));
        assert!(debug.contains("matches_expected=true"));
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_default_view_entry_id() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let entry_id =
            default_folder_view_entry_id(mailbox_guid, INBOX_FOLDER_ID, "IPF.Note").unwrap();

        let debug =
            default_folder_entry_id_values_for_debug(&[(PID_TAG_DEFAULT_VIEW_ENTRY_ID, entry_id)]);

        assert!(debug.contains("PidTagDefaultViewEntryId:bytes=70"));
        assert!(debug.contains(&format!("decoded_folder_id=0x{INBOX_FOLDER_ID:016x}")));
        assert!(debug.contains("decoded_folder_name=inbox"));
        assert!(debug.contains(&format!(
            "decoded_message_id=0x{:016x}",
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
        )));
    }

    #[test]
    fn set_property_debug_names_cover_folder_special_properties() {
        assert_eq!(
            set_property_debug_name(PID_TAG_CONTAINER_CLASS_W),
            "PidTagContainerClass"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX),
            "PidTagAdditionalRenEntryIdsEx"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_EXTENDED_FOLDER_FLAGS),
            "PidTagExtendedFolderFlags"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_FREE_BUSY_ENTRY_IDS),
            "PidTagFreeBusyEntryIds"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_SUBJECT_PREFIX_W),
            "PidTagSubjectPrefix"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS),
            "PidTagExtendedRuleMessageActions"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_SEARCH_FOLDER_ID),
            "PidTagSearchFolderId"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_SEARCH_FOLDER_STORAGE_TYPE),
            "PidTagSearchFolderStorageType"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_SEARCH_FOLDER_EFP_FLAGS),
            "PidTagSearchFolderEfpFlags"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_SEARCH_FOLDER_DEFINITION),
            "PidTagSearchFolderDefinition"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_WLINK_ENTRY_ID),
            "PidTagWlinkEntryId"
        );
        assert_eq!(
            set_property_debug_name(0x684F_0102),
            "PidTagWlinkFolderType"
        );
        assert_eq!(
            set_property_debug_name(0x6850_0102),
            "PidTagWlinkGroupClsid"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID),
            "PidTagWlinkAddressBookStoreEid"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_WLINK_CALENDAR_COLOR),
            "PidTagWlinkCalendarColor"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_EID),
            "PidTagWlinkAddressBookEid"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_WLINK_RO_GROUP_TYPE),
            "PidTagWlinkRoGroupType"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_IPM_APPOINTMENT_ENTRY_ID),
            "PidTagIpmAppointmentEntryId"
        );
        assert_eq!(
            set_property_debug_name(PID_TAG_REM_ONLINE_ENTRY_ID),
            "PidTagRemOnlineEntryId"
        );
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let values = vec![
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                CONFLICTS_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                SYNC_ISSUES_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                LOCAL_FAILURES_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                SERVER_FAILURES_FOLDER_ID,
            )
            .unwrap(),
        ];

        let debug = default_folder_entry_id_values_for_debug(&[(
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
            MapiValue::MultiBinary(values),
        )]);

        assert!(debug.contains("PidTagAdditionalRenEntryIds:count=4"));
        assert!(debug.contains("index=0"));
        assert!(debug.contains("decoded_name=conflicts"));
        assert!(debug.contains("omitted_preserved_indexes=4"));
    }

    #[test]
    fn bootstrap_query_rows_total_count_keeps_sync_issues_leaf_until_backed() {
        let object = MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: default_hierarchy_columns(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        assert_eq!(
            outlook_bootstrap_query_rows_total_count(
                Some(&object),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            ),
            Some(0)
        );
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_freebusy_data_index() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let freebusy_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            FREEBUSY_DATA_FOLDER_ID,
        )
        .unwrap();

        let debug = default_folder_entry_id_values_for_debug(&[(
            PID_TAG_FREE_BUSY_ENTRY_IDS,
            MapiValue::MultiBinary(vec![Vec::new(), Vec::new(), Vec::new(), freebusy_entry_id]),
        )]);

        assert!(debug.contains("PidTagFreeBusyEntryIds:count=4"));
        assert!(debug.contains("index=3"));
        assert!(debug.contains("decoded_name=freebusy_data"));
        assert!(debug.contains("matches_expected=true"));
    }

    #[test]
    fn default_folder_identification_values_do_not_shadow_canonical_inbox_projection() {
        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let retained = default_folder_identification_safe_property_values(
            &test_principal(),
            Some(&inbox),
            vec![
                (
                    PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                    MapiValue::MultiBinary(vec![
                        vec![0xAA],
                        vec![0xBB],
                        vec![0xCC],
                        vec![0xDD],
                        vec![0xEE],
                        vec![0xFA, 0xCE],
                    ]),
                ),
                (
                    PID_TAG_DISPLAY_NAME_W,
                    MapiValue::String("Inbox".to_string()),
                ),
            ],
        );

        assert_eq!(retained.len(), 2);
        let Some(MapiValue::MultiBinary(values)) = retained
            .iter()
            .find(|(tag, _)| *tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS)
            .map(|(_, value)| value)
        else {
            panic!("expected AdditionalRenEntryIds");
        };
        assert_eq!(values.len(), 6);
        assert_ne!(values[0], vec![0xAA]);
        assert_eq!(values[5], vec![0xFA, 0xCE]);
        assert_eq!(
            retained
                .iter()
                .find(|(tag, _)| *tag == PID_TAG_DISPLAY_NAME_W),
            Some(&(
                PID_TAG_DISPLAY_NAME_W,
                MapiValue::String("Inbox".to_string())
            ))
        );
    }

    #[test]
    fn root_scalar_default_folder_entry_ids_do_not_shadow_canonical_projection() {
        let root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            test_principal().account_id,
            CALENDAR_FOLDER_ID,
        )
        .unwrap();

        let retained = default_folder_identification_safe_property_values(
            &test_principal(),
            Some(&root),
            vec![
                (
                    PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                    MapiValue::Binary(calendar_entry_id.clone()),
                ),
                (
                    PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                    MapiValue::MultiBinary(vec![Vec::new()]),
                ),
            ],
        );

        assert!(retained.is_empty());
    }

    #[test]
    fn root_scalar_default_folder_entry_id_write_is_not_retained_as_session_state() {
        let mut root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            test_principal().account_id,
            CALENDAR_FOLDER_ID,
        )
        .unwrap();

        let values = default_folder_identification_safe_property_values(
            &test_principal(),
            Some(&root),
            vec![(
                PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                MapiValue::Binary(calendar_entry_id.clone()),
            )],
        );
        apply_mapi_property_values(Some(&mut root), values).unwrap();

        let MapiObject::Folder { properties, .. } = root else {
            panic!("expected folder object");
        };
        assert!(!properties.contains_key(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID));
    }

    #[test]
    fn ipm_subtree_ostid_write_is_retained_as_session_mutable_state() {
        let mut ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let client_ostid = vec![1; 40];

        apply_mapi_property_values(
            Some(&mut ipm_subtree),
            vec![(PID_TAG_OST_OSTID, MapiValue::Binary(client_ostid.clone()))],
        )
        .unwrap();

        let MapiObject::Folder { properties, .. } = ipm_subtree else {
            panic!("expected folder object");
        };
        assert_eq!(
            properties.get(&PID_TAG_OST_OSTID),
            Some(&MapiValue::Binary(client_ostid))
        );
    }

    #[test]
    fn logon_response_debug_summary_decodes_private_mailbox_fields() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let request = RopRequest {
            rop_id: 0xFE,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x01],
        };
        let response_buffer =
            rop_buffer_with_response(rop_logon_response_body(&principal, &request), &[42]);

        let summary = summarize_logon_response_rop(&response_buffer, &[0xFE]);

        assert!(summary.present);
        assert_eq!(summary.output_handle_index, "1");
        assert_eq!(summary.error_code, "0x00000000");
        assert_eq!(summary.logon_flags, "0x01");
        assert!(summary
            .special_folder_ids
            .starts_with(&format!("{ROOT_FOLDER_ID:#018x}")));
        assert!(summary
            .special_folder_contract
            .contains(&format!("3:ipm_subtree=0x{IPM_SUBTREE_FOLDER_ID:016x}")));
        assert!(summary
            .special_folder_contract
            .contains(&format!("4:inbox=0x{INBOX_FOLDER_ID:016x}")));
        assert!(summary.special_folder_contract_issues.is_empty());
        assert_eq!(summary.response_flags, "0x07");
        assert_eq!(summary.mailbox_guid, principal.account_id.to_string());
        assert_eq!(summary.replid, "1");
        assert_eq!(summary.replica_guid.len(), 32);
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn logon_special_folder_contract_reports_mismatched_inbox() {
        let mut folder_ids = PRIVATE_LOGON_SPECIAL_FOLDER_IDS.to_vec();
        folder_ids[4] = ROOT_FOLDER_ID;

        let issues = logon_special_folder_contract_issues(&folder_ids);

        assert!(issues.contains("4:inbox"));
        assert!(issues.contains(&format!("got=0x{ROOT_FOLDER_ID:016x}")));
        assert!(issues.contains(&format!("expected=0x{INBOX_FOLDER_ID:016x}")));
    }

    #[test]
    fn default_folder_identification_contract_decodes_root_defaults() {
        let contract = default_folder_identification_contract_for_debug(&test_principal());

        assert!(contract.contains("PidTagValidFolderMask:0x000000ff"));
        assert!(contract.contains(&format!(
            "PidTagIpmSubtreeEntryId:bytes=46:decoded_folder_id=0x{IPM_SUBTREE_FOLDER_ID:016x}"
        )));
        assert!(contract.contains(&format!(
            "PidTagCommonViewsEntryId:bytes=46:decoded_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x}"
        )));
        assert!(contract.contains("PidTagAdditionalRenEntryIds:count=5"));
        assert!(contract.contains("PidTagFreeBusyEntryIds:count=4"));
    }

    #[test]
    fn default_folder_hierarchy_projection_reports_calendar_and_contacts_identity() {
        let projection = default_folder_hierarchy_projection_for_debug(
            &test_principal(),
            &[],
            &[],
            &empty_snapshot(),
        );

        assert!(projection.contains(&format!(
            "calendar:tag=0x{PID_TAG_IPM_APPOINTMENT_ENTRY_ID:08x};folder=0x{CALENDAR_FOLDER_ID:016x}"
        )));
        assert!(projection.contains(&format!(
            "contacts:tag=0x{PID_TAG_IPM_CONTACT_ENTRY_ID:08x};folder=0x{CONTACTS_FOLDER_ID:016x}"
        )));
        assert!(projection.contains("entry_id_matches=true"));
        assert!(projection.contains("source_key_matches=true"));
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes() {
        let mut request_bytes = vec![0x02, 0x00, 0x00, 0x01];
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(CALENDAR_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        request_bytes.extend_from_slice(&[0x07, 0x00, 0x01]);
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&2u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1, u32::MAX]);

        let open_folder_request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let mut responses = rop_open_folder_response(&open_folder_request, false);
        responses.extend_from_slice(&[0x07, 0x01]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&utf16z_bytes("Calendar"));
        responses.extend_from_slice(&0u32.to_le_bytes());
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.open_folder_request_count, 1);
        assert!(summary
            .open_folder_requests
            .contains(&format!("folder=0x{CALENDAR_FOLDER_ID:016x};name=calendar")));
        assert!(summary
            .open_folder_response_shapes
            .contains("result=0x00000000;has_rules=0;is_ghosted=0"));
        assert_eq!(summary.get_properties_specific_request_count, 1);
        assert!(summary
            .get_properties_specific_requests
            .contains("tags=0x3001001f,0x36020003"));
        assert!(summary
            .get_properties_specific_response_shapes
            .contains("result=0x00000000;row=standard"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops() {
        let mut request_bytes = vec![0x07, 0x00, 0x01];
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let mut responses = vec![0x07, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&46u16.to_le_bytes());
        responses.extend_from_slice(&[0xAA; 46]);
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert!(summary
            .get_properties_specific_response_shapes
            .contains("values=0x36d00102:binary:bytes=46"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn root_default_folder_getprops_uses_canonical_projection_not_setprops_state() {
        let reopened_root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        };
        let request = get_properties_specific_request(&[PID_TAG_IPM_APPOINTMENT_ENTRY_ID]);
        let response = rop_get_properties_specific_response(
            &request,
            Some(&reopened_root),
            &test_principal(),
            &[],
            &[],
            &empty_snapshot(),
        );

        let mut cursor = Cursor::new(&response[7..]);
        assert_eq!(
            parse_property_value_for_tag(&mut cursor, PID_TAG_IPM_APPOINTMENT_ENTRY_ID).unwrap(),
            MapiValue::Binary(
                crate::mapi::identity::folder_entry_id_from_object_id(
                    test_principal().account_id,
                    CALENDAR_FOLDER_ID,
                )
                .unwrap()
            )
        );
        let MapiObject::Folder { properties, .. } = &reopened_root else {
            panic!("expected reopened root folder object");
        };
        assert!(properties.is_empty());
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_set_properties_shapes() {
        let mut property_value = Vec::new();
        property_value.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        property_value.extend_from_slice(&3u16.to_le_bytes());
        property_value.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let property_value_size = property_value.len() + 2;
        let mut request_bytes = vec![0x0A, 0x00, 0x01];
        request_bytes.extend_from_slice(&(property_value_size as u16).to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&property_value);
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let request = RopRequest {
            rop_id: 0x0A,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let response_buffer = rop_buffer_with_response(rop_set_properties_response(&request), &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.set_properties_request_count, 1);
        assert!(summary
            .set_properties_requests
            .contains("tags=0x36d00102;values=0x36d00102:binary:bytes=3"));
        assert!(summary
            .set_properties_response_shapes
            .contains("result=0x00000000;property_problem_count=0"));
        assert!(summary.parse_error.is_empty());
    }

    fn utf16z_bytes(value: &str) -> Vec<u8> {
        value
            .encode_utf16()
            .chain(std::iter::once(0))
            .flat_map(u16::to_le_bytes)
            .collect()
    }

    fn get_properties_specific_request(property_tags: &[u32]) -> RopRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
        for tag in property_tags {
            payload.extend_from_slice(&tag.to_le_bytes());
        }
        RopRequest {
            rop_id: 0x07,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        }
    }

    fn test_principal() -> AccountPrincipal {
        AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        }
    }

    fn test_mapi_session() -> MapiSession {
        let principal = test_principal();
        MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: principal.tenant_id,
            account_id: principal.account_id,
            email: principal.email,
            created_at: SystemTime::UNIX_EPOCH,
            last_seen_at: SystemTime::UNIX_EPOCH,
            first_request_type: String::new(),
            first_request_id: String::new(),
            last_request_type: String::new(),
            last_request_id: String::new(),
            request_count: 0,
            execute_request_count: 0,
            next_handle: 1,
            handles: HashMap::new(),
            message_statuses: HashMap::new(),
            message_save_generations: HashMap::new(),
            message_handle_generations: HashMap::new(),
            pending_message_recipient_replacements: HashMap::new(),
            pending_message_attachments: HashMap::new(),
            pending_attachment_parent_messages: HashMap::new(),
            pending_attachment_deletions: HashSet::new(),
            pending_embedded_message_ids: HashMap::new(),
            pending_embedded_message_attachments: HashMap::new(),
            saved_embedded_messages: HashMap::new(),
            saved_search_folder_definitions: HashMap::new(),
            special_folder_aliases: HashMap::new(),
            deleted_advertised_special_folders: HashSet::new(),
            deleted_search_folder_definitions: HashSet::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
            inbox_associated_config_stream_handles: HashSet::new(),
            inbox_rule_organizer_stream_handles: HashSet::new(),
            logon_identity: None,
            outlook_smart_input_variant: "none".to_string(),
            outlook_smart_input_variant_applied: false,
        }
    }

    fn empty_snapshot() -> MapiMailStoreSnapshot {
        MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
}
