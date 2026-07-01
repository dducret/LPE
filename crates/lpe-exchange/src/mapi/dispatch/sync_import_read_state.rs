use super::*;

pub(super) async fn append_synchronization_import_read_state_changes_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    responses: &mut Vec<u8>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x80,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let mut partial_completion = false;
    for (message_id, unread) in request.import_read_state_changes() {
        if transient_client_local_message_id(message_id) {
            continue;
        }
        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
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
