use super::*;
use lpe_storage::CancelSubmissionResult;

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
    let attachments =
        mapi_submit_attachments_from_email(store, principal.account_id, source_email).await?;
    Ok(mapi_submit_from_email(principal, source_email, attachments))
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

pub(super) fn transport_folder_response(request: &RopRequest, has_input_object: bool) -> Vec<u8> {
    if has_input_object {
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
    let has_input_object = input_object(session, handle_slots, request).is_some();
    responses.extend_from_slice(&transport_folder_response(request, has_input_object));
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
            append_spooler_advisory_dispatch_response(handle_slots, request, responses);
        }
        Some(RopId::UpdateDeferredActionMessages) => {
            append_deferred_action_messages_dispatch_response(handle_slots, request, responses);
        }
        Some(RopId::SubmitMessage | RopId::TransportSend) => {
            append_submit_message_response(
                store,
                principal,
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
            append_abort_submit_response(store, principal, request, mailboxes, emails, responses)
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

pub(super) fn spooler_advisory_response(request: &RopRequest, has_input_handle: bool) -> Vec<u8> {
    if has_input_handle {
        rop_simple_success_response(request)
    } else {
        rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_010F)
    }
}

pub(super) fn deferred_action_messages_response(
    request: &RopRequest,
    has_input_handle: bool,
) -> Vec<u8> {
    if has_input_handle {
        rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102)
    } else {
        rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_010F)
    }
}

pub(super) fn append_spooler_advisory_response(
    request: &RopRequest,
    has_input_handle: bool,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&spooler_advisory_response(request, has_input_handle));
}

pub(super) fn append_spooler_advisory_dispatch_response(
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    append_spooler_advisory_response(
        request,
        input_handle(handle_slots, request).is_some(),
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
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    append_deferred_action_messages_response(
        request,
        input_handle(handle_slots, request).is_some(),
        responses,
    );
}

pub(super) async fn append_submit_message_response<S>(
    store: &S,
    principal: &AccountPrincipal,
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
    let Some(handle) = input_handle(handle_slots, request) else {
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
                return;
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
                return;
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
                    return;
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
            return;
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
                    return;
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
            responses.extend_from_slice(&submit_success_response(request));
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

pub(super) async fn append_abort_submit_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
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
