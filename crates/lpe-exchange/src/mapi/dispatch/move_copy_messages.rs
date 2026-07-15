use super::*;

pub(super) async fn append_move_copy_messages_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if request.move_copy_want_asynchronous().is_none()
        || request.move_copy_want_copy_raw().is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let source_folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
        _ => {
            tracing::info!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x33",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                want_copy = request.move_copy_want_copy(),
                failure = "source_handle_not_folder",
                "rca debug mapi move copy messages failure"
            );
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x0000_04B9,
            ));
            return;
        }
    };
    let target_folder_id = match request
        .move_copy_target_handle(handle_slots)
        .and_then(|handle| {
            session
                .handles
                .get(&handle)
                .and_then(|object| object.folder_id())
        }) {
        Some(folder_id) => folder_id,
        None => {
            tracing::info!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x33",
                source_folder_id = format_args!("0x{source_folder_id:016x}"),
                message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                want_copy = request.move_copy_want_copy(),
                failure = "target_handle_not_folder",
                "rca debug mapi move copy messages failure"
            );
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
    };
    if let Some(partial_completion) = calendar_same_folder_move_partial_completion(
        request,
        source_folder_id,
        target_folder_id,
        snapshot,
    ) {
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    if source_folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
        tracing::info!(
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x33",
            source_folder_id = format_args!("0x{source_folder_id:016x}"),
            target_folder_id = format_args!("0x{target_folder_id:016x}"),
            message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
            want_copy = request.move_copy_want_copy(),
            failure = "recoverable_items_root_source",
            "rca debug mapi move copy messages failure"
        );
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    if matches!(source_folder_id, NOTES_FOLDER_ID | JOURNAL_FOLDER_ID) {
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            if source_folder_id == NOTES_FOLDER_ID {
                let Some(note) = snapshot.note_for_id(source_folder_id, message_id) else {
                    partial_completion = true;
                    continue;
                };
                if target_folder_id != NOTES_FOLDER_ID {
                    partial_completion = true;
                    continue;
                }
                if request.move_copy_want_copy() {
                    match store
                        .upsert_mapi_note(UpsertClientNoteInput {
                            id: None,
                            account_id: principal.account_id,
                            title: note.note.title.clone(),
                            body_text: note.note.body_text.clone(),
                            color: note.note.color.clone(),
                            categories_json: note.note.categories_json.clone(),
                        })
                        .await
                    {
                        Ok(copied) => {
                            if remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::Note,
                                copied.id,
                                None,
                                None,
                            )
                            .await
                            .is_err()
                            {
                                partial_completion = true;
                            }
                        }
                        Err(_) => partial_completion = true,
                    }
                }
                continue;
            }
            let Some(entry) = snapshot.journal_entry_for_id(source_folder_id, message_id) else {
                partial_completion = true;
                continue;
            };
            if target_folder_id != JOURNAL_FOLDER_ID {
                partial_completion = true;
                continue;
            }
            if request.move_copy_want_copy() {
                match store
                    .upsert_mapi_journal_entry(UpsertJournalEntryInput {
                        id: None,
                        account_id: principal.account_id,
                        subject: entry.entry.subject.clone(),
                        body_text: entry.entry.body_text.clone(),
                        entry_type: entry.entry.entry_type.clone(),
                        message_class: entry.entry.message_class.clone(),
                        starts_at: entry.entry.starts_at.clone(),
                        ends_at: entry.entry.ends_at.clone(),
                        occurred_at: entry.entry.occurred_at.clone(),
                        companies_json: entry.entry.companies_json.clone(),
                        contacts_json: entry.entry.contacts_json.clone(),
                    })
                    .await
                {
                    Ok(copied) => {
                        if remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::JournalEntry,
                            copied.id,
                            None,
                            None,
                        )
                        .await
                        .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                    Err(_) => partial_completion = true,
                }
            }
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    if crate::mapi_store::recoverable_storage_folder(source_folder_id).is_some() {
        if request.move_copy_want_copy() {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            let Some(item) = snapshot.recoverable_item_for_id(source_folder_id, message_id) else {
                partial_completion = true;
                continue;
            };
            if store
                .restore_recoverable_item(
                    principal.account_id,
                    item.canonical_id,
                    Some(target_mailbox.id),
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-restore-recoverable-message".to_string(),
                        subject: format!(
                            "recoverable:{}->{}",
                            item.canonical_id, target_mailbox.id
                        ),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
            }
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    if snapshot.public_folder_for_id(source_folder_id).is_some() {
        let Some(target_folder) = snapshot.public_folder_for_id(target_folder_id) else {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            let Some(item) = snapshot.public_folder_item_for_id(source_folder_id, message_id)
            else {
                partial_completion = true;
                continue;
            };
            let copied = store
                .upsert_public_folder_item(
                    UpsertPublicFolderItemInput {
                        id: None,
                        account_id: principal.account_id,
                        public_folder_id: target_folder.folder.id,
                        item_kind: item.item.item_kind.clone(),
                        message_class: item.item.message_class.clone(),
                        subject: item.item.subject.clone(),
                        body_text: item.item.body_text.clone(),
                        body_html_sanitized: item.item.body_html_sanitized.clone(),
                        source_payload_json: item.item.source_payload_json.clone(),
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: if request.move_copy_want_copy() {
                            "mapi-copy-public-folder-item".to_string()
                        } else {
                            "mapi-move-public-folder-item-copy".to_string()
                        },
                        subject: format!("{}->{}", item.item.id, target_folder.folder.id),
                    },
                )
                .await;
            let Ok(copied) = copied else {
                partial_completion = true;
                continue;
            };
            if remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::PublicFolderItem,
                copied.id,
                None,
                None,
            )
            .await
            .is_err()
            {
                partial_completion = true;
                continue;
            }
            if !request.move_copy_want_copy()
                && store
                    .delete_public_folder_item(
                        principal.account_id,
                        item.item.public_folder_id,
                        item.item.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-move-public-folder-item-delete".to_string(),
                            subject: item.item.id.to_string(),
                        },
                    )
                    .await
                    .is_err()
            {
                partial_completion = true;
            }
        }
        if !partial_completion {
            session.record_notification(MapiNotificationEvent::content(source_folder_id, None));
            session.record_notification(MapiNotificationEvent::content(target_folder_id, None));
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let mut partial_completion = false;
    for message_id in request.move_copy_message_ids() {
        let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails) else {
            partial_completion = true;
            continue;
        };
        let result = if request.move_copy_want_copy() {
            store
                .copy_jmap_email(
                    principal.account_id,
                    email.id,
                    target_mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-copy-message".to_string(),
                        subject: format!("message:{}->{}", email.id, target_mailbox.id),
                    },
                )
                .await
                .map(|_| ())
        } else {
            store
                .move_jmap_email(
                    principal.account_id,
                    email.id,
                    target_mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-move-message".to_string(),
                        subject: format!("message:{}->{}", email.id, target_mailbox.id),
                    },
                )
                .await
                .map(|_| ())
        };
        if result.is_err() {
            partial_completion = true;
        }
    }
    responses.extend_from_slice(&rop_partial_completion_response(
        0x33,
        request.response_handle_index(),
        partial_completion,
    ));
}
