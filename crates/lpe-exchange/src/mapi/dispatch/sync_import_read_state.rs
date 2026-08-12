use super::*;

pub(super) async fn append_synchronization_import_read_state_changes_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let Some(MapiObject::SynchronizationCollector {
        folder_id,
        sync_type: 0x01,
        ..
    }) = input_object(session, handle_slots, request)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x80,
            request.response_handle_index(),
            MapiError::NotSupported.as_u32(),
        ));
        return;
    };
    let folder_id = *folder_id;
    let mut changes = Vec::new();
    for (message_id, unread) in request.import_read_state_changes() {
        // [MS-OXCFXICS] section 3.2.5.9.4.6 requires read-state
        // requests for FAI messages to be ignored.
        if transient_client_local_message_id(message_id)
            || associated_sync_message_exists(folder_id, message_id, snapshot)
        {
            continue;
        }
        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
            // [MS-OXCFXICS] section 3.2.5.9.4.6 recommends rejecting a
            // predictable failure before applying any read-state changes.
            responses.extend_from_slice(&rop_error_response(
                0x80,
                request.response_handle_index(),
                MapiError::NotFound.as_u32(),
            ));
            return;
        };
        changes.push((
            email.id,
            message_id,
            mapi_mailstore::canonical_message_change_number(email),
            unread,
        ));
    }
    for (email_id, message_id, change_number, unread) in changes {
        if store
            .update_jmap_email_flags(
                principal.account_id,
                email_id,
                Some(unread),
                None,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-sync-import-read-state".to_string(),
                    subject: format!("message:{email_id}"),
                },
            )
            .await
            .is_err()
        {
            responses.extend_from_slice(&rop_error_response(
                0x80,
                request.response_handle_index(),
                MapiError::GeneralFailure.as_u32(),
            ));
            return;
        } else {
            record_sync_upload_content_change(
                session,
                folder_id,
                message_id,
                change_number,
                false,
                true,
            );
        }
    }
    // [MS-OXCROPS] section 2.2.13.3.2 defines no PartialCompletion field.
    responses.extend_from_slice(&rop_error_response(
        0x80,
        request.response_handle_index(),
        MapiError::Success.as_u32(),
    ));
}
