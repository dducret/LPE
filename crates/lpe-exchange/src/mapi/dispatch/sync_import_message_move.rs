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
    let imported_event_identity = MapiEventImportedMoveIdentity {
        expected_source_key: import_move.source_message_key.to_vec(),
        destination_source_key: import_move.destination_message_key.to_vec(),
        change_key: import_move.change_key.to_vec(),
        predecessor_change_list: import_move.predecessor_change_list.to_vec(),
    };
    let imported_message_identity = MapiMessageImportedMoveIdentity {
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
                Some(imported_event_identity),
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
    let Some(source_mailbox) = folder_row_for_id(source_folder_id, mailboxes) else {
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
    let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails) else {
        match completed_message_move_replay_identity(
            store,
            principal,
            source_folder_id,
            message_id,
            target_folder_id,
            destination_message_id,
            &imported_message_identity,
            mailboxes,
            emails,
        )
        .await
        {
            Ok(Some(identity)) => {
                crate::mapi::identity::remember_mapi_identity_with_source_key(
                    identity.canonical_id,
                    identity.object_id,
                    Some(identity.source_key),
                );
                record_sync_upload_content_checkpoint(session, source_folder_id);
                record_sync_upload_content_change(
                    session,
                    target_folder_id,
                    identity.object_id,
                    identity.change_number,
                    false,
                    false,
                );
                responses
                    .extend_from_slice(&rop_synchronization_import_message_move_response(request));
            }
            Ok(None) | Err(_) => responses.extend_from_slice(&rop_error_response(
                0x78,
                request.response_handle_index(),
                0x8004_010F,
            )),
        }
        return;
    };
    match store
        .move_jmap_email_from_mailbox_with_mapi_identity(
            principal.account_id,
            source_mailbox.id,
            email.id,
            target_mailbox.id,
            imported_message_identity,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-sync-import-move".to_string(),
                subject: format!("message:{}->{}", email.id, target_mailbox.id),
            },
        )
        .await
    {
        Ok(moved) => {
            if moved.identity.old_mapi_object_id != message_id
                || moved.identity.new_mapi_object_id != destination_message_id
                || moved.identity.old_source_key != import_move.source_message_key
                || moved.identity.new_source_key != import_move.destination_message_key
            {
                responses.extend_from_slice(&rop_error_response(
                    0x78,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            crate::mapi::identity::remember_mapi_identity_with_source_key(
                moved.email.id,
                moved.identity.new_mapi_object_id,
                Some(moved.identity.new_source_key.clone()),
            );
            record_sync_upload_content_checkpoint(session, source_folder_id);
            record_sync_upload_content_change(
                session,
                target_folder_id,
                moved.identity.new_mapi_object_id,
                moved.identity.new_change_number,
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

async fn completed_message_move_replay_identity<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    source_folder_id: u64,
    source_message_id: u64,
    target_folder_id: u64,
    destination_message_id: u64,
    imported_identity: &MapiMessageImportedMoveIdentity,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> anyhow::Result<Option<crate::store::MapiIdentityRecord>> {
    // A retry arrives after the source Outbox membership and source identity
    // have been removed. Treat it as successful only when the exact imported
    // destination identity is active and the canonical message is visible in
    // the target but no longer in the source folder.
    if crate::mapi::identity::object_id_from_source_key(&imported_identity.expected_source_key)
        != Some(source_message_id)
        || crate::mapi::identity::object_id_from_source_key(
            &imported_identity.destination_source_key,
        ) != Some(destination_message_id)
    {
        return Ok(None);
    }
    let lookup = store
        .fetch_mapi_identities_by_object_ids(principal.account_id, &[destination_message_id])
        .await?
        .into_iter()
        .find(|identity| {
            identity.object_kind == crate::store::MapiIdentityObjectKind::Message
                && identity.object_id == destination_message_id
                && identity.source_key == imported_identity.destination_source_key
        });
    let Some(lookup) = lookup else {
        return Ok(None);
    };
    let Some(email) = emails.iter().find(|email| email.id == lookup.canonical_id) else {
        return Ok(None);
    };
    if !email_matches_folder(email, target_folder_id, mailboxes)
        || email_matches_folder(email, source_folder_id, mailboxes)
    {
        return Ok(None);
    }
    let requests = [crate::store::MapiIdentityRequest {
        object_kind: crate::store::MapiIdentityObjectKind::Message,
        canonical_id: lookup.canonical_id,
        reserved_global_counter: None,
        source_key: None,
    }];
    let identity = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?
        .into_iter()
        .find(|identity| {
            identity.object_kind == crate::store::MapiIdentityObjectKind::Message
                && identity.canonical_id == lookup.canonical_id
                && identity.object_id == destination_message_id
                && identity.source_key == imported_identity.destination_source_key
                && identity.change_key == imported_identity.change_key
                && identity.predecessor_change_list == imported_identity.predecessor_change_list
        });
    Ok(identity)
}
