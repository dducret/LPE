use super::notifications::*;
use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::store_adapter::*;
use super::sync::*;
use super::tables::*;
use super::transport::*;
use super::wire::{MapiError, MapiNotificationEventMask, MapiPropertyType, MapiSyncType, RopId};
use super::*;
use crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
};
use crate::mapi::outlook_startup::{
    normal_inbox_visible_row_missing_reason, normal_inbox_visible_row_release_request_shape,
    outlook_startup_gate_summary,
};
use crate::store::{
    CommitMapiAssociatedConfigImportInput, CommitMapiNavigationShortcutCreateInput,
    CommitMapiNavigationShortcutImportInput, MapiCustomPropertyObjectKind, MapiCustomPropertyValue,
    MapiEventCreateOutcome, MapiFaiImportDisposition, MapiFaiImportedIdentity,
    MapiFolderHierarchyCommitOutcome, MapiIdentityObjectKind, MapiSpecialFolderAlias,
    MapiSyncChangeSet, MapiSyncCheckpoint, UpsertMapiAssociatedConfigInput,
    UpsertMapiNavigationShortcutInput,
};
use lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent};
use lpe_domain::current_windows_filetime;
use lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, JmapEmail, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, MapiContactCreateInput, MapiContactCustomPropertyValue,
    MapiEventAttachmentChanges, MapiEventAttachmentUpsert, MapiEventCommitInput,
    MapiEventCommitOutcome, MapiEventCreateInput, MapiEventCustomPropertyValue,
    MapiEventImportedIdentity, MapiEventImportedMoveIdentity, MapiEventReminderPatch,
    MapiMessageImportedMoveIdentity, PublicFolderPermissionInput, SearchFolderDefinition,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;

mod associated_config;
mod attachments;
mod calendar_move_copy;
mod contact_save;
mod contact_transactions;
mod contacts;
mod conversation_actions;
pub(in crate::mapi) mod custom_properties;
mod default_folders;
mod diagnostics;
mod event_save;
pub(in crate::mapi) mod event_transactions;
mod execute;
mod folder_create;
mod folder_dispatch;
mod folder_open;
mod folder_profile_mutations;
mod folders;
mod local_replica_sync;
mod logon;
mod message_dispatch;
mod message_move_copy;
mod message_open;
mod message_property_save;
mod message_save;
mod message_state;
mod messages;
mod named_properties;
mod navigation_shortcut_save;
mod notification_subscriptions;
mod object_ids;
mod permissions;
mod properties;
mod property_dispatch;
mod property_mutations;
mod property_reads;
mod property_tags;
mod public_folders;
mod recipients;
mod recoverable_items;
mod release;
mod rules;
mod search_folders;
mod stream_dispatch;
mod submission;
mod submission_overlay;
mod sync_configure;
mod sync_conflicts;
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
mod table_lifecycle;
mod table_open;
mod table_validation;
mod tables;
mod unsupported;

use associated_config::*;
use attachments::*;
use calendar_move_copy::*;
use contact_save::*;
use contact_transactions::*;
use contacts::*;
use conversation_actions::*;
use custom_properties::*;
use default_folders::*;
pub(in crate::mapi) use diagnostics::*;
use event_save::*;
use event_transactions::*;
pub(in crate::mapi) use execute::*;
use folder_create::*;
use folder_dispatch::*;
use folder_open::*;
use folder_profile_mutations::*;
use folders::*;
use local_replica_sync::*;
use logon::*;
use message_dispatch::*;
use message_move_copy::*;
use message_open::*;
use message_property_save::*;
use message_save::*;
use message_state::*;
use messages::*;
use named_properties::*;
use navigation_shortcut_save::*;
use notification_subscriptions::*;
use object_ids::*;
use permissions::*;
use properties::*;
use property_dispatch::*;
use property_mutations::*;
use property_reads::*;
use property_tags::*;
use public_folders::*;
use recipients::*;
use recoverable_items::*;
use release::*;
use rules::*;
use search_folders::*;
use stream_dispatch::*;
use submission::*;
use submission_overlay::*;
use sync_configure::*;
use sync_conflicts::*;
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
use table_lifecycle::*;
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

pub(in crate::mapi) const MAX_ROP_DEBUG_ENTRIES: usize = 32;

pub(in crate::mapi) async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &mut MapiMailStoreSnapshot,
    notification_cursor_before_snapshot: Option<i64>,
    validator: &Validator<V>,
    rop_buffer: &[u8],
    max_rop_out: u32,
    execute_flags: u32,
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
    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    let mut response_handle_indexes = Vec::new();
    let mut post_hierarchy_release_events = Vec::new();
    let mut same_execute_released_handles = HashSet::new();
    let mut same_execute_released_handle_indexes = HashSet::new();
    let mut created_emails: Vec<JmapEmail> = Vec::new();
    let mut echo_input_handle_table = false;
    let mut released_handle_indexes = Vec::new();
    let mut deferred_save_changes_response_handles = Vec::new();
    let mut default_saved_handles = HashSet::new();
    let mut chained_fast_transfer_get_buffer_request = None;
    // [MS-OXCMAPIHTTP] section 2.2.4.4.2 reports EventPending before the
    // client sends its next Execute. Preserve that delivery before processing
    // this Execute because Outlook can release its table target in that
    // request. The notification remains valid for the response that consumes
    // the pending indication.
    let (preexisting_notification_deliveries, mut delivered_notification_events) =
        session.take_pending_notification_delivery_batch();
    session.begin_execute_notification_origin_tracking();
    let preexisting_notification_targets = preexisting_notification_deliveries
        .iter()
        .filter_map(|(notification_handle, _, _)| {
            session
                .handles
                .get(notification_handle)
                .cloned()
                .map(|target| (*notification_handle, target))
        })
        .collect::<Vec<_>>();
    record_execute_stream_batch_observation(
        principal,
        request_id,
        request_rop_names,
        request_handle_table_summary,
        session,
    );
    while cursor.remaining() > 0 {
        let Some((request, logon_id)) = read_next_execute_rop_request(&mut cursor, &mut responses)
        else {
            break;
        };
        let last_client_rop = cursor.remaining_is_zero_padding();
        let typed_request = request.typed();
        let chain_fast_transfer_get_buffer = extended
            && execute_flags & EXECUTE_FLAG_CHAIN != 0
            && last_client_rop
            && matches!(
                RopId::from_u8(typed_request.rop_id()),
                Some(RopId::FastTransferSourceGetBuffer)
            )
            && request.fast_transfer_uses_server_determined_buffer_size();
        let save_changes_response_handle_target = matches!(
            RopId::from_u8(typed_request.rop_id()),
            Some(RopId::SaveChangesMessage)
        )
        .then(|| input_handle(&handle_slots, &request))
        .flatten()
        .and_then(|handle| {
            if session.pending_embedded_message_ids.contains_key(&handle) {
                Some((
                    handle,
                    SaveChangesResponseHandleTarget::EmbeddedMessage(handle),
                ))
            } else {
                session
                    .handles
                    .get(&handle)
                    .and_then(MapiObject::folder_id)
                    .map(|folder_id| {
                        (
                            handle,
                            SaveChangesResponseHandleTarget::ContainingFolder(folder_id),
                        )
                    })
            }
        });
        let mut completed_hierarchy_sync = None;
        let mut content_sync_configure_observed = false;
        let response_len_before = responses.len();
        let output_handle_before = request
            .output_handle_index()
            .and_then(|index| handle_slots.get(usize::from(index)).copied());
        if let Some(response) = unknown_property_wire_type_response(principal, &request) {
            responses.extend_from_slice(&response);
            response_handle_indexes.push(request.response_handle_index());
            break;
        }
        if let Some(error) = pre_dispatch_input_handle_error(
            session,
            &handle_slots,
            &same_execute_released_handle_indexes,
            &request,
        ) {
            responses.extend_from_slice(&pre_dispatch_input_handle_error_response(&request, error));
            response_handle_indexes.push(request.response_handle_index());
            continue;
        }
        if let Some(response) = pre_dispatch_copy_destination_handle_error_response(
            session,
            &handle_slots,
            &same_execute_released_handle_indexes,
            &request,
        ) {
            responses.extend_from_slice(&response);
            response_handle_indexes.push(request.response_handle_index());
            continue;
        }
        match RopId::from_u8(typed_request.rop_id()) {
            Some(rop_id) if is_release_dispatch_rop(rop_id) => {
                released_handle_indexes.push(request.input_handle_index().unwrap_or(0));
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
                    &mut same_execute_released_handle_indexes,
                    &mut post_hierarchy_release_events,
                )
                .await;
            }
            Some(rop_id) if is_folder_open_rop(rop_id) => {
                append_folder_open_dispatch_response(
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
                );
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
                    logon_id,
                    mailboxes,
                    emails,
                    snapshot,
                    &mut responses,
                    &mut output_handles,
                )
                .await;
            }
            Some(rop_id) if is_property_dispatch_rop(rop_id) => {
                let response_size_limit = available_execute_rop_response_size(
                    max_rop_out,
                    extended,
                    responses.len(),
                    handle_slots.len(),
                );
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
                    response_size_limit,
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
                    request_id,
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
                    max_rop_out,
                    extended,
                    chain_fast_transfer_get_buffer,
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
                    input_object(session, &handle_slots, &request),
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
                    logon_id,
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
                break;
            }
        }
        if responses.len() != response_len_before {
            response_handle_indexes.push(request.response_handle_index());
            if chain_fast_transfer_get_buffer
                && fast_transfer_source_get_buffer_response_is_partial(
                    &responses[response_len_before..],
                )
            {
                chained_fast_transfer_get_buffer_request = Some(request.clone());
            }
            if matches!(
                RopId::from_u8(typed_request.rop_id()),
                Some(RopId::SaveChangesMessage)
            ) && responses.get(response_len_before + 2..response_len_before + 6)
                == Some(&[0, 0, 0, 0])
            {
                if let Some((input_handle, target)) = save_changes_response_handle_target {
                    // [MS-OXCFXICS] 3.3.4.3.3.2.2.2 reads the saved Message
                    // state later in the same buffer. An aliased response slot
                    // also defers containing-folder projection until that read.
                    if matches!(
                        session.handles.get(&input_handle),
                        Some(MapiObject::Event { .. } | MapiObject::Contact { .. })
                    ) {
                        if save_disposition(&request) == Some(SaveDisposition::Default) {
                            default_saved_handles.insert(input_handle);
                        } else {
                            default_saved_handles.remove(&input_handle);
                        }
                    }
                    if request.input_handle_index() == Some(request.response_handle_index()) {
                        deferred_save_changes_response_handles.push((
                            request.clone(),
                            input_handle,
                            target,
                        ));
                    } else {
                        restore_save_changes_response_handle(
                            session,
                            &mut handle_slots,
                            &request,
                            target,
                        );
                    }
                }
            }
        }
        clear_released_index_after_rebind(
            session,
            &handle_slots,
            &mut same_execute_released_handle_indexes,
            output_handle_before,
            &request,
        );
        record_execute_sync_observations(
            session,
            completed_hierarchy_sync,
            content_sync_configure_observed,
        );
        if typed_request.unsupported_is_terminal() {
            break;
        }
    }
    let deferred_save_changes_response_indexes = deferred_save_changes_response_handles
        .iter()
        .map(|(request, _, _)| request.response_handle_index())
        .collect::<HashSet<_>>();
    for (request, input_handle, target) in deferred_save_changes_response_handles {
        if handle_slots
            .get(usize::from(request.response_handle_index()))
            .copied()
            == Some(input_handle)
        {
            restore_save_changes_response_handle(session, &mut handle_slots, &request, target);
        }
    }
    for handle in default_saved_handles {
        session.forget_handle(handle);
    }
    for released_handle_index in &released_handle_indexes {
        if deferred_save_changes_response_indexes.contains(released_handle_index) {
            if let Some(handle_slot) = handle_slots.get_mut(usize::from(*released_handle_index)) {
                if *handle_slot == u32::MAX {
                    *handle_slot = 0;
                }
            }
        }
    }
    // Direct events queued while processing this Execute are own-action
    // origins. The store poll below can replay the same durable change with
    // richer metadata, so retain the stable origin identities through the
    // final delivery pass without persisting client-origin state.
    let mut own_action_notification_events = session.take_execute_notification_origins();
    if session.notification_cursor.is_none() && session.has_notification_targets() {
        // [MS-OXCNOTIF] section 3.1.4.3 creates an automatic subscription
        // for an active table view. Adopt the cursor captured before this
        // request's mail-store snapshot so a concurrent delivery is replayed.
        session.notification_cursor = notification_cursor_before_snapshot;
    }
    if let Some(cursor) = session.notification_cursor {
        if let Ok(poll) = store
            .poll_mapi_notifications(principal.account_id, cursor)
            .await
        {
            let polled_event_count = poll.events.len();
            let matching_events = session.matching_notifications(poll.events);
            let matching_event_count = matching_events.len();
            for event in matching_events {
                session.record_polled_notification(event, &mut own_action_notification_events);
            }
            session.notification_cursor = poll.cursor.or(Some(cursor));
            if poll.event_pending || polled_event_count != 0 {
                tracing::debug!(
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    operation = "Execute",
                    account_id = %principal.account_id,
                    mapi_request_id = request_id,
                    notification_cursor = cursor,
                    next_notification_cursor = ?session.notification_cursor,
                    polled_event_count,
                    matching_event_count,
                    queued_event_count = session.pending_notification_count(),
                    "mapi execute polled pending notifications"
                );
            }
        }
    }
    let (notification_deliveries, mut newly_delivered_notification_events) = session
        .take_pending_notification_delivery_batch_for_execute(&own_action_notification_events);
    delivered_notification_events.append(&mut newly_delivered_notification_events);
    let new_mail_notification_delivery_count = preexisting_notification_deliveries
        .iter()
        .chain(notification_deliveries.iter())
        .filter(|(_, _, event)| {
            event.event_mask & 0x0FFF == MapiNotificationEventMask::NewMail.as_u16()
        })
        .count();
    let preexisting_notification_delivery_count =
        append_preexisting_notification_responses_with_targets(
            &mut responses,
            snapshot.identity_codec(),
            preexisting_notification_deliveries,
            &preexisting_notification_targets,
            mailboxes,
            snapshot,
            principal.account_id,
        );
    if preexisting_notification_delivery_count != 0 {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            operation = "Execute",
            account_id = %principal.account_id,
            mapi_request_id = request_id,
            notification_count = preexisting_notification_delivery_count,
            "mapi execute appended pending NotificationWait notification responses"
        );
    }
    if !notification_deliveries.is_empty() {
        let notification_targets = notification_deliveries
            .iter()
            .map(|(handle, _logon_id, event)| {
                format!(
                    "handle={handle};target={};kind={:?};event=0x{:04x};folder=0x{:016x};message={};parent={};cursor={};modseq={}",
                    if session.table_notification_active_handles.contains(handle) {
                        "table"
                    } else {
                        "subscription"
                    },
                    event.kind,
                    event.event_mask,
                    event.folder_id,
                    event
                        .message_id
                        .map(|message_id| format!("0x{message_id:016x}"))
                        .unwrap_or_else(|| "-".to_string()),
                    event
                        .parent_folder_id
                        .map(|parent_folder_id| format!("0x{parent_folder_id:016x}"))
                        .unwrap_or_else(|| "-".to_string()),
                    event
                        .change_cursor
                        .map(|cursor| cursor.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    event
                        .modseq
                        .map(|modseq| modseq.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        let mut notification_wire_shapes = Vec::new();
        for (notification_handle, logon_id, event) in notification_deliveries {
            let detailed_response = session
                .handles
                .get(&notification_handle)
                .and_then(|table| {
                    hierarchy_table_row_modified(
                        table,
                        &event,
                        mailboxes,
                        snapshot,
                        principal.account_id,
                    )
                })
                .and_then(|row| {
                    rop_hierarchy_table_row_modified_response(
                        snapshot.identity_codec(),
                        notification_handle,
                        logon_id,
                        event.event_mask,
                        row.folder_id,
                        row.insert_after_folder_id,
                        &row.row_data,
                    )
                });
            let (response, wire_shape) = match detailed_response {
                Some(response) => (Some(response), "hierarchy_table_row_modified"),
                None => (
                    rop_notify_response(
                        snapshot.identity_codec(),
                        notification_handle,
                        logon_id,
                        &event,
                    ),
                    "generic",
                ),
            };
            if let Some(response) = response {
                responses.extend_from_slice(&response);
                notification_wire_shapes
                    .push(format!("handle={notification_handle};shape={wire_shape}"));
            }
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            operation = "Execute",
            account_id = %principal.account_id,
            mapi_request_id = request_id,
            notification_count = notification_wire_shapes.len(),
            notification_targets,
            notification_wire_shapes = notification_wire_shapes.join("|"),
            "mapi execute appended RopNotify responses"
        );
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
    let response_rop_buffer = if let Some(request) = chained_fast_transfer_get_buffer_request {
        let response_handles = execute_response_handle_table(
            &responses,
            &handle_slots,
            &output_handles,
            &response_handle_indexes,
            echo_input_handle_table,
            &released_handle_indexes,
        );
        let response_payload = rop_buffer_with_response_spec(responses, &response_handles);
        let response_rop_buffer = rpc_header_ext_rop_buffer(response_payload);
        if response_rop_buffer.len() <= PACKED_FAST_TRANSFER_RESPONSE_FRAME_MAXIMUM as usize {
            let (additional_payloads, completed_hierarchy_sync) =
                packed_fast_transfer_source_get_buffer_response_payloads(
                    store,
                    principal,
                    request_id,
                    session,
                    &handle_slots,
                    &request,
                    response_rop_buffer.len(),
                    max_rop_out,
                    &response_handles,
                )
                .await;
            if !additional_payloads.is_empty() {
                record_execute_sync_observations(session, completed_hierarchy_sync, false);
                rpc_header_ext_rop_buffer_chain(response_rop_buffer, additional_payloads)
            } else {
                response_rop_buffer
            }
        } else {
            response_rop_buffer
        }
    } else {
        finalize_execute_rop_buffer(
            responses,
            &handle_slots,
            &output_handles,
            &response_handle_indexes,
            echo_input_handle_table,
            &released_handle_indexes,
            extended,
        )
    };
    restore_pending_notifications_after_execute_overflow(
        session,
        delivered_notification_events,
        &response_rop_buffer,
        max_rop_out,
    );
    if !execute_response_exceeds_max_rop_out(&response_rop_buffer, max_rop_out) {
        record_mapi_new_mail_notification_deliveries(new_mail_notification_delivery_count);
    }
    response_rop_buffer
}

#[cfg(test)]
mod tests;
