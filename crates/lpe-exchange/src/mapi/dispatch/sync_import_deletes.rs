use super::*;
use std::collections::HashSet;

fn synchronization_import_deletes_response(request: &RopRequest, had_failure: bool) -> Vec<u8> {
    // [MS-OXCROPS] section 2.2.13.5.2 and [MS-OXCFXICS] section
    // 2.2.3.2.4.5.2 define only RopId, InputHandleIndex, and ReturnValue.
    if had_failure {
        rop_error_response(0x74, request.response_handle_index(), 0x8000_4005)
    } else {
        rop_simple_success_response(request)
    }
}

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
    if hierarchy_collector_folder_id.is_some() {
        if request.import_delete_flags() & !0x03 != 0 {
            // [MS-OXCFXICS] section 3.2.5.9.4.5 recommends failing the
            // complete ROP when ImportDeleteFlags contains unknown bits.
            responses.extend_from_slice(&synchronization_import_deletes_response(request, true));
            return;
        }
        let mut seen_folder_ids = HashSet::new();
        let folder_ids = request
            .import_delete_message_ids()
            .into_iter()
            .filter(|folder_id| seen_folder_ids.insert(*folder_id))
            .collect::<Vec<_>>();
        // [MS-OXCFXICS] section 3.2.5.9.4.5 requires a reasonable
        // prediction for the entire batch and recommends failing before any
        // deletion when one cannot succeed. System folders are known from
        // the current canonical snapshot to be non-deletable.
        if folder_ids.iter().any(|folder_id| {
            folder_row_for_id(*folder_id, mailboxes).is_some_and(|mailbox| mailbox.role != "custom")
        }) {
            responses.extend_from_slice(&synchronization_import_deletes_response(request, true));
            return;
        }
        let mut had_failure = false;
        for folder_id in folder_ids {
            let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                // [MS-OXCFXICS] section 3.2.5.9.4.5: an object that was
                // already deleted MUST be ignored.
                continue;
            };
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
                had_failure = true;
                continue;
            }
        }
        responses.extend_from_slice(&synchronization_import_deletes_response(
            request,
            had_failure,
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
    if request.import_delete_flags() & !0x03 != 0 {
        responses.extend_from_slice(&synchronization_import_deletes_response(request, true));
        return;
    }
    let mut had_failure = false;
    let hard_delete = request.import_delete_hard_delete();
    let mut seen_source_keys = HashSet::new();
    let source_keys = request
        .import_delete_source_keys()
        .into_iter()
        .filter(|source_key| seen_source_keys.insert(source_key.clone()))
        .collect::<Vec<_>>();
    if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
        let unknown_source_keys = source_keys
            .iter()
            .filter(|source_key| {
                let message_id =
                    source_key_global_counter(source_key).map(crate::mapi::identity::mapi_store_id);
                let associated_config_exists = message_id
                    .and_then(|message_id| snapshot.associated_config_message_for_id(message_id))
                    .filter(|message| message.folder_id == folder_id)
                    .or_else(|| {
                        snapshot.associated_config_message_for_folder_and_source_key(
                            folder_id, source_key,
                        )
                    })
                    .is_some();
                let navigation_shortcut_exists = snapshot
                    .navigation_shortcut_messages()
                    .into_iter()
                    .find(|message| {
                        message.durable_identity.as_ref().is_some_and(|identity| {
                            identity.source_key.as_slice() == source_key.as_slice()
                        })
                    })
                    .or_else(|| {
                        message_id.and_then(|message_id| {
                            snapshot.navigation_shortcut_message_for_id(message_id)
                        })
                    })
                    .is_some();
                !associated_config_exists && !navigation_shortcut_exists
            })
            .cloned()
            .collect::<Vec<_>>();
        if !unknown_source_keys.is_empty()
            && store
                .preflight_unknown_mapi_navigation_shortcut_deletes(
                    principal.account_id,
                    folder_id,
                    &unknown_source_keys,
                )
                .await
                .is_err()
        {
            // [MS-OXCFXICS] section 3.2.5.9.4.5 recommends rejecting a
            // predictable batch failure before the first deletion because
            // the response cannot describe partial completion.
            responses.extend_from_slice(&synchronization_import_deletes_response(request, true));
            return;
        }
    }
    for source_key in source_keys {
        // [MS-OXCFXICS] sections 3.2.5.9.4.5 and 3.3.5.8.10 require
        // retries of already-applied deletions to be safe. Deduplication
        // makes the immediate same-batch form a single canonical mutation.
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
                had_failure = true;
            } else {
                record_sync_upload_content_checkpoint(session, folder_id);
            }
            continue;
        }
        if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
            // [MS-OXCFXICS] sections 2.2.3.2.4.5 and 3.3.4.3.3.2.3:
            // ImportDeletes identifies content objects by PidTagSourceKey.
            // Common Views WLinks are FAI messages, so resolve the durable
            // identity before considering the derived MID fallback.
            let message = snapshot
                .navigation_shortcut_messages()
                .into_iter()
                .find(|message| {
                    message.durable_identity.as_ref().is_some_and(|identity| {
                        identity.source_key.as_slice() == source_key.as_slice()
                    })
                })
                .or_else(|| {
                    message_id.and_then(|message_id| {
                        snapshot.navigation_shortcut_message_for_id(message_id)
                    })
                });
            if let Some(message) = message {
                if store
                    .delete_mapi_navigation_shortcut(principal.account_id, message.canonical_id)
                    .await
                    .is_err()
                {
                    had_failure = true;
                } else {
                    record_sync_upload_content_checkpoint(session, folder_id);
                }
            } else if store
                .tombstone_unknown_mapi_navigation_shortcut(
                    principal.account_id,
                    folder_id,
                    &source_key,
                )
                .await
                .is_err()
            {
                had_failure = true;
            } else {
                // [MS-OXCFXICS] section 3.2.5.9.4.5 recommends recording
                // deletions of objects absent from the server replica so a
                // later upload cannot restore them.
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
                had_failure = true;
            }
            continue;
        }
        if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
            if store
                .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                .await
                .is_err()
            {
                had_failure = true;
            }
            continue;
        }
        let Some(email) = email else {
            // [MS-OXCFXICS] section 3.2.5.9.4.5: a retry after the object
            // disappeared from this folder is a successful no-op.
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
            had_failure = true;
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
    responses.extend_from_slice(&synchronization_import_deletes_response(
        request,
        had_failure,
    ));
}
