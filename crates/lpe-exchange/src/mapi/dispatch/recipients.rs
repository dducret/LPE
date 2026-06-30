use super::*;

pub(super) fn submitted_recipients_from_pending(
    recipients: &[PendingRecipient],
) -> (
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();
    for recipient in recipients {
        let value = SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        };
        match recipient.recipient_type & 0x0F {
            0x02 => cc.push(value),
            0x03 => bcc.push(value),
            _ => to.push(value),
        }
    }
    (to, cc, bcc)
}

pub(super) fn pending_recipients_from_email(email: &JmapEmail) -> Vec<PendingRecipient> {
    message_recipients(email)
        .into_iter()
        .map(|recipient| PendingRecipient {
            row_id: recipient.order,
            recipient_type: recipient.recipient_type,
            address: recipient.address.address.clone(),
            display_name: recipient.address.display_name.clone(),
        })
        .collect()
}

pub(super) fn append_read_recipients_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if request.read_recipients_reserved() != Some(0) {
        responses.extend_from_slice(&rop_error_response(
            0x0F,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let input_handle_value = input_handle(handle_slots, request);
    let pending_recipient_object;
    let object = if let Some(recipients) = input_handle_value
        .and_then(|handle| session.pending_message_recipient_replacements.get(&handle))
    {
        let folder_id = input_object(session, handle_slots, request)
            .and_then(MapiObject::folder_id)
            .unwrap_or(INBOX_FOLDER_ID);
        pending_recipient_object = MapiObject::PendingMessage {
            folder_id,
            properties: HashMap::new(),
            recipients: recipients.clone(),
        };
        Some(&pending_recipient_object)
    } else {
        input_object(session, handle_slots, request)
    };
    responses.extend_from_slice(&rop_read_recipients_response(
        request, object, mailboxes, emails, snapshot,
    ));
}

pub(super) fn append_remove_all_recipients_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let input_handle_value = input_handle(handle_slots, request);
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::PendingMessage { recipients, .. }) => {
            recipients.clear();
            responses.extend_from_slice(&rop_simple_success_response(request));
        }
        Some(MapiObject::Message { .. }) => {
            if let Some(handle) = input_handle_value {
                session
                    .pending_message_recipient_replacements
                    .insert(handle, Vec::new());
                responses.extend_from_slice(&rop_simple_success_response(request));
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

pub(super) async fn append_modify_recipients_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let input_handle_value = input_handle(handle_slots, request);
    match input_object(session, handle_slots, request).cloned() {
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
                        input_object_mut(session, handle_slots, request)
                    else {
                        responses.extend_from_slice(&rop_error_response(
                            0x0E,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        return;
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
                    responses.extend_from_slice(&rop_simple_success_response(request));
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
                return;
            };
            let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                .or(saved_email.as_ref().map(|saved| &saved.email))
            else {
                responses.extend_from_slice(&rop_error_response(
                    0x0E,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
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
                    responses.extend_from_slice(&rop_simple_success_response(request));
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
