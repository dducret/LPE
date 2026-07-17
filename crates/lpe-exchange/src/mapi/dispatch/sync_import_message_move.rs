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
    let Some(import_move) = request.import_move() else {
        responses.extend_from_slice(&rop_error_response(
            0x78,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let source_folder_id = import_move.source_folder_id;
    let message_id = import_move.source_message_id;
    let destination_message_id = import_move.destination_message_id;
    let imported_identity = MapiEventImportedMoveIdentity {
        expected_source_key: import_move.source_message_key.to_vec(),
        destination_source_key: import_move.destination_message_key.to_vec(),
        change_key: import_move.change_key.to_vec(),
        predecessor_change_list: import_move.predecessor_change_list.to_vec(),
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
    let source_is_calendar = source_folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(source_folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar);
    if source_is_calendar {
        if target_folder_id != TRASH_FOLDER_ID {
            responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
        let Some(event) = snapshot.event_for_id(source_folder_id, message_id) else {
            responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        match store
            .move_accessible_event_to_deleted_items(
                principal.account_id,
                event.canonical_id,
                Some(imported_identity),
            )
            .await
        {
            Ok(moved) => {
                let Some(identity) = moved.principal_identity else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    return;
                };
                if identity.old_mapi_object_id != message_id
                    || identity.new_mapi_object_id != destination_message_id
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    return;
                }
                crate::mapi::identity::remember_mapi_identity_with_source_key(
                    moved.event.id,
                    identity.new_mapi_object_id,
                    Some(identity.new_source_key),
                );
                record_sync_upload_content_checkpoint(session, source_folder_id);
                record_sync_upload_content_change(
                    session,
                    target_folder_id,
                    identity.new_mapi_object_id,
                    identity.new_change_number,
                    false,
                    false,
                );
                responses
                    .extend_from_slice(&rop_synchronization_import_message_move_response(request));
            }
            Err(_) => responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            )),
        }
        return;
    }
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
