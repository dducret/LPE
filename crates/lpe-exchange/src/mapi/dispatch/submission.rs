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
