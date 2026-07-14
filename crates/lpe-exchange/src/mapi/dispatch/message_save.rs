use super::*;

pub(super) async fn append_save_changes_message_route_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    mapi_request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    created_emails: &mut Vec<JmapEmail>,
) {
    let Some(handle) = input_handle(&handle_slots, &request) else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if !save_flags_are_supported(&request) {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let save_changes_object = session.handles.get(&handle).cloned();
    session.record_recent_probe_action(format!(
        "SaveChangesMessage(in={},handle={},kind={},folder={})",
        request.input_handle_index().unwrap_or(0),
        handle,
        mapi_object_debug_kind(save_changes_object.as_ref()),
        mapi_object_debug_folder_id(save_changes_object.as_ref())
    ));
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %mapi_request_id,
        request_rop_id = "0x0c",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = handle,
        object_kind = mapi_object_debug_kind(save_changes_object.as_ref()),
        folder_id = %mapi_object_debug_folder_id(save_changes_object.as_ref()),
        save_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
        "rca debug mapi save changes before inbox probe"
    );
    match session.handles.get(&handle).cloned() {
        Some(MapiObject::CommonViewNamedView { view_id, .. }) => {
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                view_id,
            );
            return;
        }
        Some(MapiObject::PendingContact {
            folder_id,
            properties,
        }) => {
            let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            };
            let input = contact_input_from_mapi(
                principal.account_id,
                None,
                &default_contact_for_mapping(principal.account_id, &folder.collection.id),
                &properties,
            );
            match store
                .create_accessible_contact(principal.account_id, Some(&folder.collection.id), input)
                .await
            {
                Ok(contact) => {
                    let contact_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::Contact,
                        contact.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(contact_id) => contact_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::Contact,
                        contact.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session.handles.insert(
                        handle,
                        MapiObject::Contact {
                            folder_id,
                            contact_id,
                        },
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(contact_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        contact_id,
                    );
                }
                Err(error) => {
                    let (message_class, subject) = associated_config_class_and_subject(&properties);
                    let property_tags = properties.keys().copied().collect::<Vec<_>>();
                    tracing::warn!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %mapi_request_id,
                        request_rop_id = "0x0c",
                        folder_id = %format!("{folder_id:#018x}"),
                        associated_message_class = %message_class,
                        associated_subject = %subject,
                        property_tag_count = property_tags.len(),
                        property_tags = %format_debug_property_tags(&property_tags),
                        save_error = %error,
                        "rca debug failed to persist associated config message"
                    );
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                }
            }
            return;
        }
        Some(MapiObject::PendingEvent {
            folder_id,
            properties,
        }) => {
            let collection_id = match snapshot.collaboration_folder_for_id(folder_id) {
                Some(folder) => folder.collection.id.as_str(),
                None if folder_id == CALENDAR_FOLDER_ID => DEFAULT_CALENDAR_COLLECTION_ID,
                None => {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    return;
                }
            };
            let (properties, reminder_set, reminder_at) =
                match split_reminder_property_values(properties.into_iter().collect()) {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        return;
                    }
                };
            let input = match event_input_from_mapi(
                principal.account_id,
                None,
                &default_event_for_mapping(principal.account_id, collection_id),
                &properties,
            ) {
                Ok(input) => input,
                Err(_) => {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            match store
                .create_accessible_event(principal.account_id, Some(collection_id), input)
                .await
            {
                Ok(event) => {
                    let event_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::CalendarEvent,
                        event.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(event_id) => event_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if (reminder_set.is_some() || reminder_at.is_some())
                        && store
                            .update_accessible_event_reminder(
                                principal.account_id,
                                event.id,
                                reminder_set,
                                reminder_at,
                                None,
                            )
                            .await
                            .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::CalendarEvent,
                        event.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session.handles.insert(
                        handle,
                        MapiObject::Event {
                            folder_id,
                            event_id,
                        },
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(event_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        event_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PendingTask {
            folder_id,
            properties,
        }) => {
            let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            };
            let input = task_input_from_mapi(
                principal.account_id,
                None,
                &default_task_for_mapping(principal.account_id, &folder.collection.id),
                Some(&folder.collection.id),
                &properties,
            );
            match store
                .create_accessible_task(principal.account_id, input)
                .await
            {
                Ok(task) => {
                    let task_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::Task,
                        task.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(task_id) => task_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::Task,
                        task.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session
                        .handles
                        .insert(handle, MapiObject::Task { folder_id, task_id });
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(task_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        task_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PendingNote {
            folder_id,
            properties,
        }) => {
            let input = note_input_from_mapi(
                principal.account_id,
                None,
                &default_note_for_mapping(),
                &properties,
            );
            match store.upsert_mapi_note(input).await {
                Ok(note) => {
                    let note_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::Note,
                        note.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(note_id) => note_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::Note,
                        note.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session
                        .handles
                        .insert(handle, MapiObject::Note { folder_id, note_id });
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        note_id,
                        mapi_mailstore::change_number_for_store_id(note_id),
                        false,
                        true,
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(note_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        note_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PendingJournalEntry {
            folder_id,
            properties,
        }) => {
            let input = journal_entry_input_from_mapi(
                principal.account_id,
                None,
                &default_journal_entry_for_mapping(),
                &properties,
            );
            match store.upsert_mapi_journal_entry(input).await {
                Ok(entry) => {
                    let journal_entry_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::JournalEntry,
                        entry.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(journal_entry_id) => journal_entry_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::JournalEntry,
                        entry.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session.handles.insert(
                        handle,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id,
                        },
                    );
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        journal_entry_id,
                        mapi_mailstore::change_number_for_store_id(journal_entry_id),
                        false,
                        true,
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(journal_entry_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        journal_entry_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PendingConversationAction {
            folder_id,
            properties,
        }) => {
            let action = conversation_action_from_mapi_properties(&properties);
            if action.conversation_id.is_nil() {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                return;
            }
            let move_target_mailbox_id = conversation_action_target_mailbox_id(&action, mailboxes);
            let input = lpe_storage::UpsertConversationActionInput {
                account_id: principal.account_id,
                conversation_id: action.conversation_id,
                subject: action.subject,
                categories_json: action.categories_json,
                move_folder_entry_id: action.move_folder_entry_id,
                move_store_entry_id: action.move_store_entry_id,
                move_target_mailbox_id,
                max_delivery_time: action.max_delivery_time,
                last_applied_time: action.last_applied_time,
                version: Some(action.version),
                processed: Some(action.processed),
            };
            match store.upsert_conversation_action(input).await {
                Ok(saved) => {
                    let conversation_action_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::ConversationAction,
                        saved.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(conversation_action_id) => conversation_action_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    if apply_conversation_action_to_existing_messages(
                        store, principal, &saved, mailboxes, emails,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session.handles.insert(
                        handle,
                        MapiObject::ConversationAction {
                            folder_id,
                            conversation_action_id,
                        },
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(conversation_action_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        conversation_action_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PendingNavigationShortcut {
            folder_id,
            properties,
        }) => {
            let shortcut =
                navigation_shortcut_from_mapi_properties(principal.account_id, None, &properties);
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %mapi_request_id,
                request_rop_id = "0x0c",
                folder_id = format_args!("0x{:016x}", folder_id),
                decoded_shortcut =
                    %common_views_saved_shortcut_summary(&shortcut, &properties),
                "rca debug mapi common views navigation shortcut save"
            );
            let input = UpsertMapiNavigationShortcutInput {
                // [MS-OXCMSG] sections 2.2.3.2 and 2.2.3.3: a message
                // created by RopCreateMessage receives a new identity when it
                // is first saved, even when it replaces a logical WLink.
                id: Some(Uuid::new_v4()),
                account_id: principal.account_id,
                subject: shortcut.subject,
                target_folder_id: shortcut.target_folder_id,
                shortcut_type: shortcut.shortcut_type,
                flags: shortcut.flags,
                save_stamp: shortcut.save_stamp,
                section: shortcut.section,
                ordinal: shortcut.ordinal,
                group_header_id: shortcut.group_header_id,
                group_name: shortcut.group_name,
            };
            match store.upsert_mapi_navigation_shortcut(input).await {
                Ok(saved) => {
                    session.record_last_post_hierarchy_create_save_object_context(format!(
                        "kind=navigation_shortcut;send_candidate=false;create_associated=true;class=IPM.Microsoft.WunderBar.Link;request_id={mapi_request_id};folder=0x{folder_id:016x};role={};subject={};target_folder={};shortcut_type={};section={};ordinal={};group_name={};canonical_id={}",
                        debug_role_for_folder_id(folder_id),
                        saved.subject,
                        saved.target_folder_id
                            .map(|id| format!("0x{id:016x}"))
                            .unwrap_or_else(|| "none".to_string()),
                        saved.shortcut_type,
                        saved.section,
                        saved.ordinal,
                        saved.group_name,
                        saved.id
                    ));
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %mapi_request_id,
                        request_rop_id = "0x0c",
                        folder_id = format_args!("0x{:016x}", folder_id),
                        navigation_shortcut_id = %saved.id,
                        subject = %saved.subject,
                        target_folder_id = saved
                            .target_folder_id
                            .map(|id| format!("0x{id:016x}"))
                            .unwrap_or_else(|| "none".to_string()),
                        shortcut_type = saved.shortcut_type,
                        section = saved.section,
                        ordinal = saved.ordinal,
                        group_name = %saved.group_name,
                        "rca debug persisted navigation shortcut"
                    );
                    let shortcut_id = match remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::NavigationShortcut,
                        saved.id,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(shortcut_id) => shortcut_id,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            return;
                        }
                    };
                    session.handles.insert(
                        handle,
                        MapiObject::NavigationShortcut {
                            folder_id,
                            shortcut_id,
                        },
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(shortcut_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        shortcut_id,
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::Event { folder_id, .. })
            if mapi_calendar_content_items_suppressed(folder_id, snapshot) =>
        {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
        Some(MapiObject::PendingMessage { .. })
            if session.pending_embedded_message_ids.contains_key(&handle) =>
        {
            let message_id = session
                .pending_embedded_message_ids
                .get(&handle)
                .copied()
                .unwrap_or(0);
            if let Some(MapiObject::PendingMessage { properties, .. }) =
                session.handles.get(&handle).cloned()
            {
                if let Some(attachment_key) = session
                    .pending_embedded_message_attachments
                    .get(&handle)
                    .copied()
                {
                    session
                        .saved_embedded_messages
                        .insert(attachment_key, properties);
                }
            }
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                message_id,
            );
            return;
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            pending_properties,
        }) => {
            let staged_property_write = !pending_properties.is_empty();
            let staged_recipient_replacement = session
                .pending_message_recipient_replacements
                .get(&handle)
                .cloned();
            let pending = session
                .pending_attachment_deletions
                .iter()
                .filter_map(|(pending_folder_id, pending_message_id, attach_num)| {
                    (*pending_folder_id == folder_id && *pending_message_id == message_id)
                        .then_some(*attach_num)
                })
                .collect::<Vec<_>>();
            let has_pending_changes = staged_property_write
                || staged_recipient_replacement.is_some()
                || !pending.is_empty();
            let force_save = request.payload.first().copied().unwrap_or(0) & 0x04 != 0;
            let current_generation = session.message_save_generation(folder_id, message_id);
            let handle_generation = session
                .message_handle_generation(handle)
                .unwrap_or(current_generation);
            if has_pending_changes && handle_generation != current_generation && !force_save {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_0109,
                ));
                session.handles.insert(
                    handle,
                    MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email,
                        pending_properties,
                    },
                );
                return;
            }
            if staged_property_write
                && apply_staged_message_property_values(
                    store,
                    principal,
                    folder_id,
                    message_id,
                    saved_email.clone(),
                    pending_properties.clone(),
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await
                .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                session.handles.insert(
                    handle,
                    MapiObject::Message {
                        folder_id,
                        message_id,
                        saved_email,
                        pending_properties,
                    },
                );
                return;
            }
            if let Some(recipients) = staged_recipient_replacement.as_deref() {
                if apply_staged_message_recipient_replacement(
                    store, principal, folder_id, message_id, recipients, mailboxes, emails,
                )
                .await
                .is_err()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    session.handles.insert(
                        handle,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                            saved_email,
                            pending_properties,
                        },
                    );
                    return;
                }
            }
            let mut delete_failed = false;
            for attach_num in pending.iter().copied() {
                let Some(attachment) =
                    snapshot.attachment_for_message(folder_id, message_id, attach_num)
                else {
                    session
                        .pending_attachment_deletions
                        .remove(&(folder_id, message_id, attach_num));
                    continue;
                };
                match store
                    .delete_message_attachment(
                        principal.account_id,
                        &attachment.file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-attachment".to_string(),
                            subject: attachment.file_reference.clone(),
                        },
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        session
                            .pending_attachment_deletions
                            .remove(&(folder_id, message_id, attach_num));
                    }
                    _ => {
                        delete_failed = true;
                        break;
                    }
                }
            }
            if delete_failed {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            if !pending.is_empty() {
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(message_id),
                ));
                record_sync_upload_content_change(
                    session,
                    folder_id,
                    message_id,
                    mapi_mailstore::change_number_for_store_id(message_id),
                    false,
                    true,
                );
            }
            if staged_property_write {
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(message_id),
                ));
            }
            if staged_recipient_replacement.is_some() {
                session
                    .pending_message_recipient_replacements
                    .remove(&handle);
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(message_id),
                ));
            }
            if has_pending_changes {
                session.record_message_saved(handle, folder_id, message_id);
            }
            session.handles.insert(
                handle,
                MapiObject::Message {
                    folder_id,
                    message_id,
                    saved_email,
                    pending_properties: HashMap::new(),
                },
            );
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                message_id,
            );
            return;
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => {
            let pending = session
                .pending_attachment_deletions
                .iter()
                .filter_map(|(pending_folder_id, pending_message_id, attach_num)| {
                    (*pending_folder_id == folder_id && *pending_message_id == event_id)
                        .then_some(*attach_num)
                })
                .collect::<Vec<_>>();
            let mut delete_failed = false;
            for attach_num in pending.iter().copied() {
                let Some(attachment) =
                    snapshot.attachment_for_message(folder_id, event_id, attach_num)
                else {
                    session
                        .pending_attachment_deletions
                        .remove(&(folder_id, event_id, attach_num));
                    continue;
                };
                match store
                    .delete_calendar_event_attachment(
                        principal.account_id,
                        &attachment.file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-calendar-attachment".to_string(),
                            subject: attachment.file_reference.clone(),
                        },
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        session
                            .pending_attachment_deletions
                            .remove(&(folder_id, event_id, attach_num));
                    }
                    _ => {
                        delete_failed = true;
                        break;
                    }
                }
            }
            if delete_failed {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            if !pending.is_empty() {
                session
                    .record_notification(MapiNotificationEvent::content(folder_id, Some(event_id)));
                record_sync_upload_content_change(
                    session,
                    folder_id,
                    event_id,
                    mapi_mailstore::change_number_for_store_id(event_id),
                    false,
                    true,
                );
            }
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                event_id,
            );
            return;
        }
        Some(MapiObject::Contact { contact_id, .. })
        | Some(MapiObject::Task {
            task_id: contact_id,
            ..
        })
        | Some(MapiObject::Note {
            note_id: contact_id,
            ..
        })
        | Some(MapiObject::JournalEntry {
            journal_entry_id: contact_id,
            ..
        })
        | Some(MapiObject::ConversationAction {
            conversation_action_id: contact_id,
            ..
        })
        | Some(MapiObject::NavigationShortcut {
            shortcut_id: contact_id,
            ..
        })
        | Some(MapiObject::AssociatedConfig {
            config_id: contact_id,
            ..
        })
        | Some(MapiObject::DelegateFreeBusyMessage {
            message_id: contact_id,
            ..
        }) => {
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                contact_id,
            );
            return;
        }
        Some(MapiObject::PendingAssociatedMessage {
            folder_id,
            properties,
        }) => {
            match persist_associated_config_message(store, principal, folder_id, &properties).await
            {
                Ok((saved, message_id)) => {
                    session.record_last_post_hierarchy_create_save_object_context(format!(
                        "kind=associated_config;send_candidate=false;create_associated=true;request_id={mapi_request_id};folder=0x{folder_id:016x};role={};class={};subject={};mapi_message_id=0x{message_id:016x};canonical_id={};property_count={}",
                        debug_role_for_folder_id(folder_id),
                        saved.message_class,
                        saved.subject,
                        saved.id,
                        saved.properties_json.as_object().map_or(0, |properties| properties.len())
                    ));
                    session.handles.insert(
                        handle,
                        MapiObject::AssociatedConfig {
                            folder_id,
                            config_id: message_id,
                            saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
                                id: message_id,
                                folder_id,
                                canonical_id: saved.id,
                                message_class: saved.message_class.clone(),
                                subject: saved.subject.clone(),
                                properties_json: saved.properties_json.clone(),
                            }),
                        },
                    );
                    record_sync_upload_content_change(
                        session,
                        folder_id,
                        message_id,
                        mapi_mailstore::change_number_for_store_id(message_id),
                        true,
                        false,
                    );
                    session.record_notification(MapiNotificationEvent::content(
                        folder_id,
                        Some(message_id),
                    ));
                    append_save_changes_message_response(
                        session,
                        responses,
                        handle_slots,
                        &request,
                        handle,
                        message_id,
                    );
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %mapi_request_id,
                        request_rop_id = "0x0c",
                        folder_id = %format!("{folder_id:#018x}"),
                        associated_config_id = %saved.id,
                        mapi_message_id = %format!("{message_id:#018x}"),
                        associated_message_class = %saved.message_class,
                        associated_subject = %saved.subject,
                        property_count = saved.properties_json.as_object().map_or(0, |properties| properties.len()),
                        "rca debug persisted associated config message"
                    );
                }
                Err(_) => responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            }
            return;
        }
        Some(MapiObject::PublicFolderItem {
            folder_id,
            item_id,
            properties,
        }) => {
            if !properties.is_empty()
                && apply_canonical_public_folder_item_property_values(
                    store,
                    principal,
                    folder_id,
                    item_id,
                    properties.into_iter().collect(),
                    snapshot,
                )
                .await
                .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            session.record_notification(MapiNotificationEvent::content(folder_id, Some(item_id)));
            record_sync_upload_content_change(
                session,
                folder_id,
                item_id,
                mapi_mailstore::change_number_for_store_id(item_id),
                false,
                true,
            );
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                item_id,
            );
            return;
        }
        _ => {}
    }
    let Some(MapiObject::PendingMessage {
        folder_id,
        properties,
        recipients,
    }) = session.handles.get(&handle).cloned()
    else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        if !recipients.is_empty() {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let input = UpsertPublicFolderItemInput {
            id: None,
            account_id: principal.account_id,
            public_folder_id: folder.folder.id,
            item_kind: "post".to_string(),
            message_class: optional_pending_text_property(&properties, &[PID_TAG_MESSAGE_CLASS_W])
                .unwrap_or_else(|| "IPM.Post".to_string()),
            subject: pending_text_property(
                &properties,
                &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
            ),
            body_text: pending_text_property(&properties, &[PID_TAG_BODY_W]),
            body_html_sanitized: pending_html_property(&properties),
            source_payload_json: json!({"source": "mapi-save-message"}).to_string(),
        };
        match store
            .upsert_public_folder_item(
                input,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-save-public-folder-item".to_string(),
                    subject: format!("public-folder:{}", folder.folder.id),
                },
            )
            .await
        {
            Ok(item) => {
                let item_id = match remember_created_mapi_identity(
                    store,
                    principal,
                    MapiIdentityObjectKind::PublicFolderItem,
                    item.id,
                    None,
                    None,
                )
                .await
                {
                    Ok(item_id) => item_id,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                };
                session.handles.insert(
                    handle,
                    MapiObject::PublicFolderItem {
                        folder_id,
                        item_id,
                        properties: HashMap::new(),
                    },
                );
                session
                    .record_notification(MapiNotificationEvent::content(folder_id, Some(item_id)));
                record_sync_upload_content_change(
                    session,
                    folder_id,
                    item_id,
                    mapi_mailstore::change_number_for_store_id(item_id),
                    false,
                    true,
                );
                append_save_changes_message_response(
                    session,
                    responses,
                    handle_slots,
                    &request,
                    handle,
                    item_id,
                );
            }
            Err(_) => responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            )),
        }
        return;
    }
    let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if pending_message_is_trash_sync_artifact(folder_id, &properties, &recipients) {
        let message_id = transient_associated_message_id(folder_id, &properties);
        append_save_changes_message_response(
            session,
            responses,
            handle_slots,
            &request,
            handle,
            message_id,
        );
        return;
    }
    if pending_message_is_sync_metadata_only(&properties, &recipients) {
        if folder_id == TRASH_FOLDER_ID {
            let message_id = transient_associated_message_id(folder_id, &properties);
            session.handles.insert(
                handle,
                MapiObject::Message {
                    folder_id,
                    message_id,
                    saved_email: None,
                    pending_properties: HashMap::new(),
                },
            );
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                message_id,
            );
            return;
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x0c",
            input_handle_index = request.input_handle_index.unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = "pending_message",
            folder_id = %format!("{folder_id:#018x}"),
            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
            property_tag_count = properties.len(),
            property_tags = %format_debug_property_tags(
                &properties.keys().copied().collect::<Vec<_>>()
            ),
            save_rejected_reason = "sync_metadata_only",
            "rca debug mapi save changes message"
        );
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let pending_attachments = session
        .pending_message_attachments
        .get(&handle)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(_, attachment)| attachment)
        .collect::<Vec<_>>();
    let input = jmap_import_from_pending_message(
        principal,
        mailbox,
        &properties,
        &recipients,
        pending_attachments,
    );
    let imported_source_key = imported_message_source_key(&properties);
    let imported_source_key_global_counter = imported_source_key
        .as_deref()
        .and_then(source_key_global_counter);
    match store
        .import_jmap_email(
            input,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-save-message".to_string(),
                subject: format!("folder:{}", mailbox.id),
            },
        )
        .await
    {
        Ok(email) => {
            session.pending_message_attachments.remove(&handle);
            session
                .pending_attachment_parent_messages
                .retain(|_, parent_handle| *parent_handle != handle);
            if apply_conversation_actions_to_new_message(
                store, principal, mailboxes, &email, snapshot,
            )
            .await
            .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            let (message_id, preserved_import_source_key, identity_fallback_reason) =
                match remember_created_message_mapi_identity(
                    store,
                    principal,
                    email.id,
                    imported_source_key.clone(),
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            input_handle_index = request.input_handle_index.unwrap_or(0),
                            response_handle_index = request.response_handle_index(),
                            object_kind = "message",
                            folder_id = %format!("{folder_id:#018x}"),
                            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                            imported_source_key_global_counter = imported_source_key_global_counter
                                .map(|counter| counter.to_string())
                                .unwrap_or_default(),
                            imported_source_key = %imported_source_key
                                .as_deref()
                                .map(bytes_to_hex)
                                .unwrap_or_default(),
                            identity_error = %error,
                            "rca debug mapi save changes message identity"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                };
            if upsert_custom_property_values_from_map(
                store,
                principal,
                MapiCustomPropertyObjectKind::Message,
                email.id,
                &properties,
            )
            .await
            .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            session.handles.insert(
                handle,
                MapiObject::Message {
                    folder_id,
                    message_id,
                    saved_email: Some(MapiSavedEmail {
                        email: email.clone(),
                    }),
                    pending_properties: HashMap::new(),
                },
            );
            let associated = matches!(
                properties.get(&PID_TAG_ASSOCIATED),
                Some(MapiValue::Bool(true))
            );
            let message_class =
                optional_pending_text_property(&properties, &[PID_TAG_MESSAGE_CLASS_W])
                    .unwrap_or_else(|| "IPM.Note".to_string());
            let subject = pending_text_property(
                &properties,
                &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
            );
            let sender_name = optional_pending_text_property(&properties, &[PID_TAG_SENDER_NAME_W])
                .unwrap_or_default();
            let sender_email =
                optional_pending_text_property(&properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
                    .unwrap_or_default();
            let to_count = recipients
                .iter()
                .filter(|recipient| !matches!(recipient.recipient_type, 0x02 | 0x03))
                .count();
            let cc_count = recipients
                .iter()
                .filter(|recipient| recipient.recipient_type == 0x02)
                .count();
            let bcc_count = recipients
                .iter()
                .filter(|recipient| recipient.recipient_type == 0x03)
                .count();
            record_sync_upload_content_change(
                session,
                folder_id,
                message_id,
                mapi_mailstore::canonical_message_change_number(&email),
                associated,
                !associated,
            );
            let canonical_email_id = email.id;
            created_emails.push(email);
            session
                .record_notification(MapiNotificationEvent::content(folder_id, Some(message_id)));
            session.record_last_post_hierarchy_create_save_object_context(format!(
                "kind=message;send_candidate=true;create_associated={associated};request_id={mapi_request_id};folder=0x{folder_id:016x};role={};class={message_class};subject={subject};sender_name={sender_name};sender_email={sender_email};recipient_count={};to_count={to_count};cc_count={cc_count};bcc_count={bcc_count};mapi_message_id=0x{message_id:016x};canonical_id={}",
                role_for_folder_id(folder_id).unwrap_or(""),
                recipients.len(),
                canonical_email_id
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x0c",
                input_handle_index = request.input_handle_index.unwrap_or(0),
                response_handle_index = request.response_handle_index(),
                object_kind = "message",
                folder_id = %format!("{folder_id:#018x}"),
                folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                item_id = %format!("{message_id:#018x}"),
                imported_source_key_global_counter = imported_source_key_global_counter
                    .map(|counter| counter.to_string())
                    .unwrap_or_default(),
                imported_source_key = %imported_source_key
                    .as_deref()
                    .map(bytes_to_hex)
                    .unwrap_or_default(),
                preserved_import_source_key,
                identity_fallback_reason = %identity_fallback_reason,
                "rca debug mapi save changes message"
            );
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                &request,
                handle,
                message_id,
            );
        }
        Err(error) => {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x0c",
                input_handle_index = request.input_handle_index.unwrap_or(0),
                response_handle_index = request.response_handle_index(),
                object_kind = "pending_message",
                folder_id = %format!("{folder_id:#018x}"),
                folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                recipient_count = recipients.len(),
                save_error = %error,
                "rca debug mapi save changes message"
            );
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ))
        }
    }
}
