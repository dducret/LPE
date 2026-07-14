use super::*;

pub(super) async fn append_synchronization_import_deletes_response<S: ExchangeStore>(
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
    let hierarchy_collector_folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            sync_type: 0x02,
            ..
        }) => Some(*folder_id),
        _ => None,
    };
    if let Some(collector_folder_id) = hierarchy_collector_folder_id {
        let mut partial_completion = false;
        for folder_id in request.import_delete_message_ids() {
            let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                partial_completion = true;
                continue;
            };
            if mailbox.role != "custom" {
                partial_completion = true;
                continue;
            }
            if store
                .destroy_jmap_mailbox(
                    principal.account_id,
                    mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-sync-import-delete-folder".to_string(),
                        subject: format!("folder:{}", mailbox.id),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
                continue;
            }
            record_sync_upload_hierarchy_change(session, collector_folder_id, folder_id);
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x74,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x74,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let mut partial_completion = false;
    let hard_delete = request.import_delete_hard_delete();
    for source_key in request.import_delete_source_keys() {
        let message_id =
            source_key_global_counter(&source_key).map(crate::mapi::identity::mapi_store_id);
        if let Some(message) = message_id
            .and_then(|message_id| snapshot.associated_config_message_for_id(message_id))
            .filter(|message| message.folder_id == folder_id)
            .or_else(|| {
                snapshot.associated_config_message_for_folder_and_source_key(folder_id, &source_key)
            })
        {
            if store
                .delete_mapi_associated_config(principal.account_id, message.canonical_id)
                .await
                .is_err()
            {
                partial_completion = true;
            } else {
                record_sync_upload_content_checkpoint(session, folder_id);
            }
            continue;
        }
        let Some(message_id) = message_id else {
            continue;
        };
        let email = message_for_id(folder_id, message_id, mailboxes, emails);
        if transient_client_local_message_id(message_id) && email.is_none() {
            continue;
        }
        if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
            if store
                .delete_mapi_note(principal.account_id, note.canonical_id)
                .await
                .is_err()
            {
                partial_completion = true;
            }
            continue;
        }
        if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
            if store
                .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                .await
                .is_err()
            {
                partial_completion = true;
            }
            continue;
        }
        let Some(email) = email else {
            partial_completion = true;
            continue;
        };
        let result = if hard_delete
            || email.mailbox_role == "trash"
            || mailbox_is_trash_or_descendant(email.mailbox_id, mailboxes)
        {
            store
                .delete_jmap_email_from_mailbox(
                    principal.account_id,
                    email.mailbox_id,
                    email.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-sync-import-hard-delete".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await
                .map(|_| ())
        } else if let Some(trash_mailbox) = mailboxes.iter().find(|mailbox| mailbox.role == "trash")
        {
            store
                .move_jmap_email_from_mailbox(
                    principal.account_id,
                    email.mailbox_id,
                    email.id,
                    trash_mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-sync-import-soft-delete".to_string(),
                        subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                    },
                )
                .await
                .map(|_| ())
        } else {
            store
                .delete_jmap_email_from_mailbox(
                    principal.account_id,
                    email.mailbox_id,
                    email.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-sync-import-delete-without-trash".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await
                .map(|_| ())
        };
        if result.is_err() {
            partial_completion = true;
        } else {
            record_sync_upload_content_change(
                session,
                folder_id,
                message_id,
                mapi_mailstore::canonical_message_change_number(email),
                false,
                false,
            );
        }
    }
    responses.extend_from_slice(&rop_partial_completion_response(
        0x74,
        request.response_handle_index(),
        partial_completion,
    ));
}
