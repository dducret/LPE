use super::*;
use lpe_storage::{
    CancelSubmissionResult, SubmissionMessageCustomPropertyInput, SubmissionSourcePatch,
};
use std::collections::HashMap;
use uuid::Uuid;

use crate::mapi::{
    identity::{
        global_counter_from_store_id, object_ids_from_message_entry_id, source_key_for_object_id,
        FIRST_DYNAMIC_GLOBAL_COUNTER, OUTBOX_FOLDER_ID, SENT_FOLDER_ID,
    },
    properties::{MapiValue, PID_TAG_TARGET_ENTRY_ID},
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct OptimizedSendTarget {
    message_id: u64,
    global_counter: u64,
    source_key: Vec<u8>,
}

struct PersistedSubmissionSource {
    message_id: Uuid,
    source_folder_id: u64,
    source_object_id: u64,
    patch: Option<SubmissionSourcePatch>,
}

fn optimized_send_target(
    properties: &HashMap<u32, MapiValue>,
    account_id: Uuid,
) -> std::result::Result<Option<OptimizedSendTarget>, &'static str> {
    let Some(value) = properties.get(&PID_TAG_TARGET_ENTRY_ID) else {
        return Ok(None);
    };
    let MapiValue::Binary(entry_id) = value else {
        return Err("optimized_send_target_not_binary");
    };
    let Some((folder_id, message_id)) = object_ids_from_message_entry_id(account_id, entry_id)
    else {
        return Err("optimized_send_target_entry_id_invalid");
    };
    if folder_id != OUTBOX_FOLDER_ID {
        return Err("optimized_send_target_not_outbox");
    }
    let Some(global_counter) = global_counter_from_store_id(message_id) else {
        return Err("optimized_send_target_message_id_invalid");
    };
    if global_counter < FIRST_DYNAMIC_GLOBAL_COUNTER {
        return Err("optimized_send_target_message_id_not_dynamic");
    }
    Ok(Some(OptimizedSendTarget {
        message_id,
        global_counter,
        source_key: source_key_for_object_id(message_id),
    }))
}

async fn optimized_send_replay_email<S>(
    store: &S,
    account_id: Uuid,
    target: &OptimizedSendTarget,
) -> Result<Option<(JmapEmail, u64)>>
where
    S: ExchangeStore,
{
    let identities = store
        .fetch_mapi_identities_by_object_ids(account_id, &[target.message_id])
        .await?;
    let mut identity = identities.into_iter().find(|identity| {
        identity.object_kind == MapiIdentityObjectKind::Message
            && identity.object_id == target.message_id
            && identity.source_key == target.source_key
    });
    if identity.is_none() {
        identity = store
            .fetch_mapi_identities_by_source_keys(
                account_id,
                std::slice::from_ref(&target.source_key),
            )
            .await?
            .into_iter()
            .find(|identity| {
                identity.object_kind == MapiIdentityObjectKind::Message
                    && identity.source_key == target.source_key
            });
    }
    let Some(identity) = identity else {
        return Ok(None);
    };
    let message_id = identity.object_id;
    let emails = store
        .fetch_jmap_emails(account_id, &[identity.canonical_id])
        .await?;
    Ok(emails
        .into_iter()
        .find(|email| email.id == identity.canonical_id && abort_submit_source_is_sent(email))
        .map(|email| (email, message_id)))
}

pub(super) async fn mapi_submit_from_existing_email<S>(
    store: &S,
    principal: &AccountPrincipal,
    email: &JmapEmail,
) -> Result<SubmitMessageInput>
where
    S: ExchangeStore,
{
    let protected_emails = store
        .fetch_jmap_emails_with_protected_bcc(principal.account_id, &[email.id])
        .await?;
    let protected_email = protected_emails.iter().find(|loaded| loaded.id == email.id);
    let source_email = protected_email.unwrap_or(email);
    Ok(mapi_submit_from_email(principal, source_email, Vec::new()))
}

pub(super) fn submit_success_response(request: &RopRequest) -> Vec<u8> {
    if request.rop_id == 0x4A {
        rop_transport_send_success_response(request)
    } else {
        rop_simple_success_response(request)
    }
}

pub(super) fn submit_source_is_outgoing(email: &JmapEmail) -> bool {
    matches!(email.mailbox_role.as_str(), "drafts" | "outbox")
}

pub(super) fn submit_audit_entry(principal: &AccountPrincipal, handle: u32) -> AuditEntryInput {
    AuditEntryInput {
        actor: principal.email.clone(),
        action: "mapi-submit-message".to_string(),
        subject: format!("handle:{handle}"),
    }
}

pub(super) fn submitted_message_handle_object(
    submitted: &SubmittedMessage,
    mailboxes: &[JmapMailbox],
    message_id: u64,
) -> MapiObject {
    MapiObject::Message {
        folder_id: submitted_mapi_folder_id(submitted, mailboxes),
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    }
}

pub(super) fn transport_folder_response(request: &RopRequest, has_private_logon: bool) -> Vec<u8> {
    if has_private_logon {
        rop_get_transport_folder_response(request)
    } else {
        rop_error_response(0x6D, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn options_data_response(request: &RopRequest, has_input_object: bool) -> Vec<u8> {
    if has_input_object {
        rop_options_data_response(request)
    } else {
        rop_error_response(0x6F, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn append_transport_folder_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let has_private_logon = exact_private_logon_request_handle(session, handle_slots, request);
    responses.extend_from_slice(&transport_folder_response(request, has_private_logon));
}

pub(super) fn append_options_data_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let has_input_object = input_object(session, handle_slots, request).is_some();
    responses.extend_from_slice(&options_data_response(request, has_input_object));
}

pub(super) fn append_transport_info_dispatch_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetTransportFolder) => {
            append_transport_folder_response(session, handle_slots, request, responses);
        }
        Some(RopId::OptionsData) => {
            append_options_data_response(session, handle_slots, request, responses);
        }
        _ => {}
    }
}

pub(super) fn is_submission_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::SetSpooler
            | RopId::SpoolerLockMessage
            | RopId::TransportNewMail
            | RopId::UpdateDeferredActionMessages
            | RopId::SubmitMessage
            | RopId::TransportSend
            | RopId::AbortSubmit
            | RopId::GetTransportFolder
            | RopId::OptionsData
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_submission_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    mapi_request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    responses: &mut Vec<u8>,
    created_emails: &mut Vec<JmapEmail>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SetSpooler | RopId::SpoolerLockMessage | RopId::TransportNewMail) => {
            append_spooler_advisory_dispatch_response(session, handle_slots, request, responses);
        }
        Some(RopId::UpdateDeferredActionMessages) => {
            append_deferred_action_messages_dispatch_response(
                session,
                handle_slots,
                request,
                responses,
            );
        }
        Some(RopId::SubmitMessage | RopId::TransportSend) => {
            append_submit_message_response(
                store,
                principal,
                mapi_request_id,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                created_emails,
                responses,
            )
            .await;
        }
        Some(RopId::AbortSubmit) => {
            append_abort_submit_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                responses,
            )
            .await;
        }
        Some(RopId::GetTransportFolder | RopId::OptionsData) => {
            append_transport_info_dispatch_response(session, handle_slots, request, responses);
        }
        _ => {}
    }
}

pub(super) fn abort_submit_source_is_sent(email: &JmapEmail) -> bool {
    email.mailbox_role == "sent"
        || email
            .mailbox_states
            .iter()
            .any(|state| state.role == "sent")
}

pub(super) async fn abort_submit_canonical_message_id<S>(
    store: &S,
    account_id: Uuid,
    folder_id: u64,
    message_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Option<Uuid>
where
    S: ExchangeStore,
{
    if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
        return abort_submit_source_is_sent(email).then_some(email.id);
    }
    store
        .fetch_mapi_identities_by_object_ids(account_id, &[message_id])
        .await
        .ok()?
        .into_iter()
        .find(|identity| identity.object_kind == MapiIdentityObjectKind::Message)
        .map(|identity| identity.canonical_id)
}

pub(super) fn abort_submit_cancel_response(
    request: &RopRequest,
    result: anyhow::Result<CancelSubmissionResult>,
) -> Vec<u8> {
    match result {
        Ok(CancelSubmissionResult::Cancelled | CancelSubmissionResult::AlreadyCancelled) => {
            rop_simple_success_response(request)
        }
        Ok(CancelSubmissionResult::NotFound) => {
            rop_error_response(0x34, request.response_handle_index(), 0x8004_010F)
        }
        Ok(CancelSubmissionResult::NotCancellable) | Err(_) => {
            rop_error_response(0x34, request.response_handle_index(), 0x8004_0102)
        }
    }
}

pub(super) fn spooler_advisory_response(request: &RopRequest, has_private_logon: bool) -> Vec<u8> {
    if has_private_logon {
        rop_simple_success_response(request)
    } else {
        rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn deferred_action_messages_response(
    request: &RopRequest,
    _has_private_logon: bool,
) -> Vec<u8> {
    rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102)
}

pub(super) fn append_spooler_advisory_response(
    request: &RopRequest,
    has_private_logon: bool,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&spooler_advisory_response(request, has_private_logon));
}

pub(super) fn append_spooler_advisory_dispatch_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    append_spooler_advisory_response(
        request,
        exact_private_logon_request_handle(session, handle_slots, request),
        responses,
    );
}

pub(super) fn append_deferred_action_messages_response(
    request: &RopRequest,
    has_input_handle: bool,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&deferred_action_messages_response(
        request,
        has_input_handle,
    ));
}

pub(super) fn append_deferred_action_messages_dispatch_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    if !exact_private_logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NotSupported.as_u32(),
        ));
        return;
    }
    append_deferred_action_messages_response(request, true, responses);
}

pub(super) async fn append_submit_message_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    mapi_request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    created_emails: &mut Vec<JmapEmail>,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let submit_rop_name = if request.rop_id == RopId::TransportSend.as_u8() {
        "TransportSend"
    } else {
        "SubmitMessage"
    };
    let Some(handle) = input_handle(handle_slots, request) else {
        session.record_post_hierarchy_submit_attempt_context(format!(
            "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=missing_input_handle;input_handle=none;send_attempt=true"
        ));
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
        return;
    };
    let Some(object) = session.handles.get(&handle).cloned() else {
        session.record_post_hierarchy_submit_attempt_context(format!(
            "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=session_handle_not_found;input_handle={handle};send_attempt=true"
        ));
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
        return;
    };
    let optimized_send_target = match &object {
        MapiObject::PendingMessage { properties, .. } => {
            optimized_send_target(properties, principal.account_id)
        }
        MapiObject::Message {
            pending_properties, ..
        } => optimized_send_target(pending_properties, principal.account_id),
        _ => Ok(None),
    };
    let optimized_send_target = match optimized_send_target {
        Ok(target) => target,
        Err(failure_reason) => {
            session.record_post_hierarchy_submit_attempt_context(format!(
                "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason={failure_reason};input_handle={handle};send_attempt=false"
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = %format!("{:#04x}", request.rop_id),
                input_handle = handle,
                failure_reason,
                "rca debug mapi submit message"
            );
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8007_0057,
            ));
            return;
        }
    };
    let direct_pending_source = matches!(&object, MapiObject::PendingMessage { .. });
    if let Some(target) = optimized_send_target.as_ref() {
        match optimized_send_replay_email(store, principal.account_id, target).await {
            Ok(Some((email, message_id))) => {
                let folder_id = mailboxes
                    .iter()
                    .find(|mailbox| mailbox.id == email.mailbox_id)
                    .map(mapi_folder_id)
                    .unwrap_or(SENT_FOLDER_ID);
                session.record_post_hierarchy_submit_attempt_context(format!(
                    "request_id={mapi_request_id};rop={submit_rop_name};result=optimized_send_replay_success;input_handle={handle};message_id=0x{message_id:016x};canonical_message_id={};send_attempt=true",
                    email.id
                ));
                session.handles.insert(
                    handle,
                    MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email: None,
                        pending_properties: HashMap::new(),
                    },
                );
                created_emails.push(email);
                responses.extend_from_slice(&submit_success_response(request));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                session.record_post_hierarchy_submit_attempt_context(format!(
                    "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=optimized_send_replay_lookup_failed;input_handle={handle};message_id=0x{:016x};lookup_error={error};send_attempt=true",
                    target.message_id
                ));
                responses.extend_from_slice(&rop_error_response(
                    request.rop_id,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
        }
    }
    let optimized_send_outbox_mailbox_id = if optimized_send_target.is_some() {
        let Some(outbox_mailbox_id) =
            folder_row_for_id(OUTBOX_FOLDER_ID, mailboxes).map(|mailbox| mailbox.id)
        else {
            session.record_post_hierarchy_submit_attempt_context(format!(
                "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=optimized_send_outbox_missing;input_handle={handle};send_attempt=false"
            ));
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        Some(outbox_mailbox_id)
    } else {
        None
    };
    let (input, persisted_source) = match object {
        MapiObject::PendingMessage {
            properties,
            recipients,
            ..
        } => match mapi_submit_from_pending_message(principal, &properties, &recipients) {
            Ok(mut input) => {
                let mut staged_attachments = session
                    .pending_message_attachments
                    .get(&handle)
                    .cloned()
                    .unwrap_or_default();
                staged_attachments.sort_by_key(|(attach_num, _)| *attach_num);
                let mut attachments = staged_attachments
                    .into_iter()
                    .map(|(_, attachment)| attachment)
                    .collect::<Vec<_>>();
                attachments.append(&mut input.attachments);
                input.attachments = attachments;
                (input, None)
            }
            Err(error) => {
                session.record_post_hierarchy_submit_attempt_context(format!(
                    "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=invalid_meeting_scheduling_fields;input_handle={handle};submit_input_error={error};send_attempt=false"
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = %format!("{:#04x}", request.rop_id),
                    input_handle = handle,
                    failure_reason = "invalid_meeting_scheduling_fields",
                    submit_input_error = %error,
                    "rca debug mapi submit message"
                );
                responses.extend_from_slice(&rop_error_response(
                    request.rop_id,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return;
            }
        },
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            pending_properties,
        } => {
            let Some(email) = saved_email
                .as_ref()
                .map(|saved| &saved.email)
                .or_else(|| message_for_id(folder_id, message_id, mailboxes, emails))
            else {
                session.record_post_hierarchy_submit_attempt_context(format!(
                    "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=message_identity_not_found;input_handle={handle};object_kind=message;folder=0x{folder_id:016x};role={};message_id=0x{message_id:016x};send_attempt=true",
                    role_for_folder_id(folder_id).unwrap_or("")
                ));
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
                return;
            };
            if !submit_source_is_outgoing(email) {
                session.record_post_hierarchy_submit_attempt_context(format!(
                    "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=message_not_in_outgoing_folder;input_handle={handle};object_kind=message;folder=0x{folder_id:016x};role={};message_id=0x{message_id:016x};mailbox_role={};send_attempt=true",
                    role_for_folder_id(folder_id).unwrap_or(""),
                    email.mailbox_role
                ));
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
                return;
            }
            let recipient_replacement = session
                .pending_message_recipient_replacements
                .get(&handle)
                .cloned();
            let deleted_property_tags = session
                .pending_message_property_deletions
                .get(&handle)
                .cloned()
                .unwrap_or_default();
            let mut staged_attachments = session
                .pending_message_attachments
                .get(&handle)
                .cloned()
                .unwrap_or_default();
            staged_attachments.sort_by_key(|(attach_num, _)| *attach_num);
            let added_attachments = staged_attachments
                .into_iter()
                .map(|(_, attachment)| attachment)
                .collect::<Vec<_>>();
            let has_pending_attachment_deletions = session.pending_attachment_deletions.keys().any(
                |(pending_folder_id, pending_message_id, _)| {
                    *pending_folder_id == folder_id && *pending_message_id == message_id
                },
            );
            let mut delete_attachment_ids = session
                .pending_attachment_deletions
                .iter()
                .filter_map(
                    |((pending_folder_id, pending_message_id, _), attachment_id)| {
                        (*pending_folder_id == folder_id && *pending_message_id == message_id)
                            .then_some(*attachment_id)
                    },
                )
                .collect::<Vec<_>>();
            let selected_scheduling_attachment_id = email
                .calendar_meeting_request
                .as_ref()
                .and_then(|request| request.transport_attachment_id)
                .or_else(|| {
                    email
                        .calendar_meeting_response
                        .as_ref()
                        .and_then(|response| response.transport_attachment_id)
                });
            let selected_scheduling_attachment_deleted = selected_scheduling_attachment_id
                .is_some_and(|attachment_id| delete_attachment_ids.contains(&attachment_id));
            let has_overlay = !pending_properties.is_empty()
                || recipient_replacement.is_some()
                || !deleted_property_tags.is_empty()
                || !added_attachments.is_empty()
                || has_pending_attachment_deletions;

            let result = if has_overlay {
                saved_message_submission_overlay(
                    store,
                    principal,
                    email,
                    &pending_properties,
                    &deleted_property_tags,
                    recipient_replacement.as_deref(),
                    added_attachments,
                    selected_scheduling_attachment_deleted,
                )
                .await
                .map(|overlay| {
                    if let Some(attachment_id) = overlay.replaced_scheduling_attachment_id {
                        delete_attachment_ids.push(attachment_id);
                    }
                    delete_attachment_ids.sort_unstable();
                    delete_attachment_ids.dedup();
                    let mut delete_custom_property_tags = deleted_property_tags
                        .iter()
                        .copied()
                        .filter(|tag| is_custom_property_tag(*tag))
                        .collect::<Vec<_>>();
                    delete_custom_property_tags.sort_unstable();
                    let patch = SubmissionSourcePatch {
                        expected_source_modseq: Some(email.modseq),
                        delete_attachment_ids,
                        custom_property_upserts: overlay
                            .custom_property_upserts
                            .into_iter()
                            .map(|value| SubmissionMessageCustomPropertyInput {
                                property_tag: value.property_tag,
                                property_type: value.property_type,
                                property_value: value.property_value,
                            })
                            .collect(),
                        delete_custom_property_tags,
                        canonical_followup_update: overlay.followup_update,
                    };
                    (overlay.input, Some(patch))
                })
            } else {
                mapi_submit_from_existing_email(store, principal, email)
                    .await
                    .map(|input| (input, None))
            };
            match result {
                Ok((input, patch)) => (
                    input,
                    Some(PersistedSubmissionSource {
                        message_id: email.id,
                        source_folder_id: folder_id,
                        source_object_id: message_id,
                        patch,
                    }),
                ),
                Err(error) => {
                    session.record_post_hierarchy_submit_attempt_context(format!(
                        "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=existing_message_submit_input_failed;input_handle={handle};object_kind=message;folder=0x{folder_id:016x};role={};message_id=0x{message_id:016x};submit_input_error={};send_attempt=true",
                        role_for_folder_id(folder_id).unwrap_or(""),
                        error
                    ));
                    warn!(
                        error = %error,
                        "failed to build canonical input for MAPI draft submit"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    return;
                }
            }
        }
        _ => {
            session.record_post_hierarchy_submit_attempt_context(format!(
                "request_id={mapi_request_id};rop={submit_rop_name};result=error;failure_reason=unsupported_object_for_submit;input_handle={handle};object_kind={};folder={};send_attempt=true",
                mapi_object_debug_kind(Some(&object)),
                mapi_object_debug_folder_id(Some(&object))
            ));
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
            return;
        }
    };
    let submit_attempt_context = format!(
        "request_id={mapi_request_id};rop={submit_rop_name};input_handle={handle};subject={};to_count={};cc_count={};bcc_count={};attachment_count={};body_text_bytes={};body_html_bytes={};draft_message_id={};source={};send_attempt=true",
        input.subject,
        input.to.len(),
        input.cc.len(),
        input.bcc.len(),
        input.attachments.len(),
        input.body_text.len(),
        input
            .body_html_sanitized
            .as_deref()
            .map(str::len)
            .unwrap_or(0),
        input
            .draft_message_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        input.source
    );
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
    let saved_overlay_source = persisted_source
        .as_ref()
        .filter(|source| source.patch.is_some())
        .map(|source| (source.source_folder_id, source.source_object_id));
    let submit_result = if let Some(source) = persisted_source {
        if let Some(patch) = source.patch {
            store
                .submit_message_with_source_patch(
                    input,
                    patch,
                    submit_audit_entry(principal, handle),
                )
                .await
        } else {
            store
                .submit_draft_message(
                    principal.account_id,
                    source.message_id,
                    principal.account_id,
                    "mapi-submit-message",
                    submit_audit_entry(principal, handle),
                )
                .await
        }
    } else {
        store
            .submit_message(input, submit_audit_entry(principal, handle))
            .await
    };
    match submit_result {
        Ok(submitted) => {
            if direct_pending_source {
                session.pending_message_attachments.remove(&handle);
                session
                    .pending_attachment_parent_messages
                    .retain(|_, parent_handle| *parent_handle != handle);
            }
            if let Some((source_folder_id, source_object_id)) = saved_overlay_source {
                session
                    .pending_message_recipient_replacements
                    .remove(&handle);
                session.pending_message_property_deletions.remove(&handle);
                session.pending_message_attachments.remove(&handle);
                session
                    .pending_attachment_parent_messages
                    .retain(|_, parent_handle| *parent_handle != handle);
                session
                    .pending_attachment_deletions
                    .retain(|(folder_id, message_id, _), _| {
                        *folder_id != source_folder_id || *message_id != source_object_id
                    });
            }
            session.record_post_hierarchy_submit_attempt_context(format!(
                "{submit_attempt_context};result=canonical_submit_success;submitted_message_id={}",
                submitted.message_id
            ));
            let identity_result = if let Some(target) = optimized_send_target.as_ref() {
                remember_created_mapi_identity(
                    store,
                    principal,
                    MapiIdentityObjectKind::Message,
                    submitted.message_id,
                    Some(target.global_counter),
                    Some(target.source_key.clone()),
                )
                .await
            } else {
                remember_created_mapi_identity(
                    store,
                    principal,
                    MapiIdentityObjectKind::Message,
                    submitted.message_id,
                    None,
                    None,
                )
                .await
            };
            let message_id = match identity_result {
                Ok(message_id) => {
                    if let Some(target) = optimized_send_target.as_ref() {
                        if message_id == target.message_id {
                            if let Some(outbox_mailbox_id) = optimized_send_outbox_mailbox_id {
                                if let Err(error) = store
                                    .mirror_jmap_email_into_mailbox(
                                        principal.account_id,
                                        submitted.message_id,
                                        outbox_mailbox_id,
                                        AuditEntryInput {
                                            actor: principal.email.clone(),
                                            action: "mapi-optimized-send-outbox-mirror".to_string(),
                                            subject: format!(
                                                "message:{};outbox_message_id:{message_id:#018x}",
                                                submitted.message_id
                                            ),
                                        },
                                    )
                                    .await
                                {
                                    session.record_post_hierarchy_submit_attempt_context(format!(
                                        "{submit_attempt_context};result=degraded_success;degradation_reason=optimized_send_outbox_mirror_failed;submitted_message_id={};mirror_error={error}",
                                        submitted.message_id
                                    ));
                                    tracing::info!(
                                        rca_debug = true,
                                        adapter = "mapi",
                                        endpoint = "emsmdb",
                                        mailbox = %principal.email,
                                        request_type = "Execute",
                                        request_rop_id = %format!("{:#04x}", request.rop_id),
                                        input_handle = handle,
                                        submitted_message_id = %submitted.message_id,
                                        mirror_error = %error,
                                        failure_reason = "optimized_send_outbox_mirror_failed",
                                        "rca debug mapi submit message"
                                    );
                                }
                            }
                        } else {
                            session.record_post_hierarchy_submit_attempt_context(format!(
                                "{submit_attempt_context};result=degraded_success;degradation_reason=optimized_send_target_identity_mismatch;submitted_message_id={};expected_message_id=0x{:016x};actual_message_id=0x{message_id:016x}",
                                submitted.message_id,
                                target.message_id
                            ));
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = %format!("{:#04x}", request.rop_id),
                                input_handle = handle,
                                submitted_message_id = %submitted.message_id,
                                expected_message_id = %format!("{:#018x}", target.message_id),
                                actual_message_id = %format!("{message_id:#018x}"),
                                failure_reason = "optimized_send_target_identity_mismatch",
                                "rca debug mapi submit message"
                            );
                        }
                    }
                    Some(message_id)
                }
                Err(error) => {
                    let failure_reason = if optimized_send_target.is_some() {
                        "optimized_send_target_identity_reservation_failed"
                    } else {
                        "submitted_message_identity_allocation_failed"
                    };
                    session.record_post_hierarchy_submit_attempt_context(format!(
                        "{submit_attempt_context};result=degraded_success;degradation_reason={failure_reason};submitted_message_id={};identity_error={error}",
                        submitted.message_id
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = %format!("{:#04x}", request.rop_id),
                        input_handle = handle,
                        submitted_message_id = %submitted.message_id,
                        identity_error = %error,
                        failure_reason,
                        "rca debug mapi submit message"
                    );
                    session.forget_handle(handle);
                    None
                }
            };
            if let Some(message_id) = message_id {
                session.handles.insert(
                    handle,
                    submitted_message_handle_object(&submitted, mailboxes, message_id),
                );
                match store
                    .fetch_jmap_emails(principal.account_id, &[submitted.message_id])
                    .await
                {
                    Ok(mut emails) => created_emails.append(&mut emails),
                    Err(error) => {
                        session.record_post_hierarchy_submit_attempt_context(format!(
                            "{submit_attempt_context};result=degraded_success;degradation_reason=submitted_message_same_execute_load_failed;submitted_message_id={};load_error={error}",
                            submitted.message_id
                        ));
                        tracing::info!(
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
                        );
                    }
                }
            }
            responses.extend_from_slice(&submit_success_response(request));
        }
        // Canonical submission failed before commit, so returning an error is
        // retry-safe. The successful arm above must never return a ROP error.
        Err(error) => {
            session.record_post_hierarchy_submit_attempt_context(format!(
                "{submit_attempt_context};result=error;failure_reason=canonical_submit_failed;submit_error={}",
                error
            ));
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

pub(super) async fn append_abort_submit_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if !exact_private_logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            0x34,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(folder_id) = request.abort_submit_folder_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x34,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    };
    let Some(message_id) = request.abort_submit_message_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x34,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
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
        return;
    };
    let Some(canonical_message_id) = canonical_message_id else {
        responses.extend_from_slice(&rop_error_response(
            0x34,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let cancel_result = store
        .cancel_queued_submission(
            principal.account_id,
            canonical_message_id,
            abort_submit_audit_entry(principal, canonical_message_id),
        )
        .await;
    responses.extend_from_slice(&abort_submit_cancel_response(request, cancel_result));
}

pub(super) fn abort_submit_audit_entry(
    principal: &AccountPrincipal,
    canonical_message_id: Uuid,
) -> AuditEntryInput {
    AuditEntryInput {
        actor: principal.email.clone(),
        action: "mapi-abort-submit".to_string(),
        subject: format!("message:{canonical_message_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimized_send_target_is_validated_before_submission() {
        let account_id = Uuid::from_u128(0x1234);
        let mut properties = HashMap::new();
        assert!(optimized_send_target(&properties, account_id)
            .expect("an absent target is an ordinary submission")
            .is_none());

        properties.insert(PID_TAG_TARGET_ENTRY_ID, MapiValue::U32(1));
        assert_eq!(
            optimized_send_target(&properties, account_id),
            Err("optimized_send_target_not_binary")
        );

        properties.insert(PID_TAG_TARGET_ENTRY_ID, MapiValue::Binary(vec![0xFF; 16]));
        assert_eq!(
            optimized_send_target(&properties, account_id),
            Err("optimized_send_target_entry_id_invalid")
        );

        let message_id = crate::mapi::identity::mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + 7);
        let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            OUTBOX_FOLDER_ID,
            message_id,
        )
        .expect("valid optimized-send target EntryID");
        properties.insert(PID_TAG_TARGET_ENTRY_ID, MapiValue::Binary(entry_id));
        let target = optimized_send_target(&properties, account_id)
            .expect("valid target")
            .expect("optimized target");
        assert_eq!(target.message_id, message_id);
        assert_eq!(target.global_counter, FIRST_DYNAMIC_GLOBAL_COUNTER + 7);
    }
}
