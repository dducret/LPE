use super::*;

pub(super) async fn append_synchronization_import_message_move_response<S: ExchangeStore>(
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
    let Some((source_folder_id, message_id)) = request.import_move() else {
        responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let Some(target_folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if snapshot.note_for_id(source_folder_id, message_id).is_some() {
        if target_folder_id == NOTES_FOLDER_ID {
            record_sync_upload_content_checkpoint(session, source_folder_id);
            responses.extend_from_slice(&rop_synchronization_import_message_move_response(request));
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            ));
        }
        return;
    }
    if snapshot
        .journal_entry_for_id(source_folder_id, message_id)
        .is_some()
    {
        if target_folder_id == JOURNAL_FOLDER_ID {
            record_sync_upload_content_checkpoint(session, source_folder_id);
            responses.extend_from_slice(&rop_synchronization_import_message_move_response(request));
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            ));
        }
        return;
    }
    let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails) else {
        responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
        responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
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
                    return;
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
            responses.extend_from_slice(&rop_synchronization_import_message_move_response(request));
        }
        Err(_) => responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_010F,
        )),
    }
}
