use super::*;

pub(super) async fn append_synchronization_import_message_change_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(folder_id) =
        input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x72,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let property_values = match request.import_property_values() {
        Ok(values) => values,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
    };
    let import_flag = request.import_flag().unwrap_or_default();
    let import_property_tags = property_values
        .iter()
        .map(|(tag, _)| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let import_source_key = property_values
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => Some(bytes_to_hex(bytes)),
            _ => None,
        })
        .unwrap_or_default();
    let import_source_key_global_counter =
        imported_property_source_key_global_counter(&property_values);
    let import_source_key_identity_scope = import_source_key_global_counter
        .map(import_source_key_identity_scope)
        .unwrap_or("");
    let message_id = request.import_message_id().unwrap_or(0);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x72",
        folder_id = format_args!("0x{:016x}", folder_id),
        folder_role = debug_role_for_folder_id(folder_id),
        folder_container_class = debug_container_class_for_folder_id(folder_id),
        import_flag = format_args!("0x{import_flag:02x}"),
        import_associated = import_flag & 0x10 != 0,
        import_fail_on_conflict = import_flag & 0x40 != 0,
        import_property_tag_count = property_values.len(),
        import_property_tags = %import_property_tags,
        import_source_key = %import_source_key,
        import_source_key_global_counter = import_source_key_global_counter
            .map(|counter| counter.to_string())
            .unwrap_or_default(),
        import_source_key_identity_scope,
        parsed_message_id = format_args!("0x{message_id:016x}"),
        "rca debug mapi sync import message change"
    );
    if import_flag & 0x10 != 0 && folder_id == COMMON_VIEWS_FOLDER_ID {
        let properties = property_values.into_iter().collect::<HashMap<_, _>>();
        let shortcut =
            navigation_shortcut_from_mapi_properties(principal.account_id, None, &properties);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x72",
            folder_id = format_args!("0x{:016x}", folder_id),
            decoded_shortcut =
                %common_views_saved_shortcut_summary(&shortcut, &properties),
            "rca debug mapi common views navigation shortcut import"
        );
        match store
            .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                id: None,
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
            })
            .await
        {
            Ok(saved) => {
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
                            0x72,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::NavigationShortcut {
                        folder_id,
                        shortcut_id,
                    },
                );
                set_handle_slot(handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_import_message_change_response(
                    &request,
                ));
                output_handles.push(handle);
            }
            Err(_) => responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_010F,
            )),
        }
        return;
    }
    if import_flag & 0x10 != 0 {
        let pending_object = MapiObject::PendingAssociatedMessage {
            folder_id,
            properties: property_values.into_iter().collect(),
        };
        let handle = session.allocate_output_handle(request.output_handle_index, pending_object);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
        return;
    }
    if message_id != 0 {
        if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
            let properties = property_values.into_iter().collect::<HashMap<_, _>>();
            let mut imported_identity = match imported_event_identity_from_properties(&properties) {
                Ok(Some(identity)) => identity,
                Ok(None) | Err(_) => {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            let imported_last_modification_time =
                match imported_event_last_modification_filetime(&properties) {
                    Ok(value) => value,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        return;
                    }
                };
            if imported_identity.source_key != event.source_key {
                responses.extend_from_slice(&rop_error_response(
                    0x72,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            let relation = match sync_import_version_relation(
                &imported_identity.predecessor_change_list,
                &event.version.predecessor_change_list,
            ) {
                Ok(relation) => relation,
                Err(_) => {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            let import_disposition = match relation {
                SyncImportVersionRelation::Newer => MapiEventImportDisposition::Apply,
                SyncImportVersionRelation::OlderOrSame => {
                    MapiEventImportDisposition::IgnoreOlderOrSame
                }
                SyncImportVersionRelation::Conflict if import_flag & 0x40 != 0 => {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_0802,
                    ));
                    return;
                }
                SyncImportVersionRelation::Conflict => {
                    let merged_pcl = match merge_sync_predecessor_change_lists(
                        &event.version.predecessor_change_list,
                        &imported_identity.predecessor_change_list,
                    ) {
                        Ok(pcl) => pcl,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x72,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            return;
                        }
                    };
                    let current_last_modification_time =
                        mapi_mailstore::filetime_from_rfc3339_utc(&event.version.updated_at);
                    let imported_wins = match imported_version_wins_last_writer(
                        imported_last_modification_time,
                        &imported_identity.change_key,
                        current_last_modification_time,
                        &event.version.change_key,
                    ) {
                        Ok(imported_wins) => imported_wins,
                        Err(_) => {
                            responses.extend_from_slice(&rop_error_response(
                                0x72,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            return;
                        }
                    };
                    imported_identity.predecessor_change_list = merged_pcl;
                    if imported_wins {
                        MapiEventImportDisposition::Apply
                    } else {
                        imported_identity.change_key = event.version.change_key.clone();
                        MapiEventImportDisposition::KeepServerContent
                    }
                }
            };
            let mut transaction = MapiEventTransaction::new(0x01, event.version.canonical_modseq);
            transaction.import_disposition = import_disposition;
            if import_disposition != MapiEventImportDisposition::IgnoreOlderOrSame {
                transaction.imported_identity = Some(imported_identity);
            }
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Event {
                    folder_id,
                    event_id: message_id,
                    transaction,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses
                .extend_from_slice(&rop_synchronization_import_message_change_response(request));
            output_handles.push(handle);
            return;
        }
    }
    if message_id != 0 && message_for_id(folder_id, message_id, mailboxes, emails).is_some() {
        let change_number = message_for_id(folder_id, message_id, mailboxes, emails)
            .map(mapi_mailstore::canonical_message_change_number)
            .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message_id));
        if import_flag & 0x40 != 0
            && import_message_change_conflicts_with_current_pcl(&property_values, change_number)
        {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0109,
            ));
            return;
        }
        if apply_canonical_message_property_values(
            store,
            principal,
            folder_id,
            message_id,
            property_values,
            mailboxes,
            emails,
        )
        .await
        .is_err()
        {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Message {
                folder_id,
                message_id,
                saved_email: None,
                pending_properties: HashMap::new(),
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        record_sync_upload_content_change(
            session,
            folder_id,
            message_id,
            change_number,
            import_flag & 0x10 != 0,
            import_flag & 0x10 == 0,
        );
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
    } else if message_id != 0
        && snapshot
            .public_folder_item_for_id(folder_id, message_id)
            .is_some()
    {
        if apply_canonical_public_folder_item_property_values(
            store,
            principal,
            folder_id,
            message_id,
            property_values,
            snapshot,
        )
        .await
        .is_err()
        {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::PublicFolderItem {
                folder_id,
                item_id: message_id,
                properties: HashMap::new(),
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        record_sync_upload_content_change(
            session,
            folder_id,
            message_id,
            mapi_mailstore::change_number_for_store_id(message_id),
            false,
            true,
        );
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
    } else if message_id != 0 && snapshot.note_for_id(folder_id, message_id).is_some() {
        if apply_canonical_note_property_values(
            store,
            principal,
            folder_id,
            message_id,
            property_values,
            snapshot,
        )
        .await
        .is_err()
        {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Note {
                folder_id,
                note_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        record_sync_upload_content_change(
            session,
            folder_id,
            message_id,
            mapi_mailstore::change_number_for_store_id(message_id),
            false,
            true,
        );
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
    } else if message_id != 0
        && snapshot
            .journal_entry_for_id(folder_id, message_id)
            .is_some()
    {
        if apply_canonical_journal_entry_property_values(
            store,
            principal,
            folder_id,
            message_id,
            property_values,
            snapshot,
        )
        .await
        .is_err()
        {
            responses.extend_from_slice(&rop_error_response(
                0x72,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::JournalEntry {
                folder_id,
                journal_entry_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        record_sync_upload_content_change(
            session,
            folder_id,
            message_id,
            mapi_mailstore::change_number_for_store_id(message_id),
            false,
            true,
        );
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
    } else {
        let pending_object = match snapshot
            .collaboration_folder_for_id(folder_id)
            .map(|folder| folder.kind)
        {
            Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                folder_id,
                properties: property_values.into_iter().collect(),
            },
            None if folder_id == CALENDAR_FOLDER_ID => MapiObject::PendingEvent {
                folder_id,
                properties: property_values.into_iter().collect(),
            },
            _ if folder_id == NOTES_FOLDER_ID => MapiObject::PendingNote {
                folder_id,
                properties: property_values.into_iter().collect(),
            },
            _ if folder_id == JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                folder_id,
                properties: property_values.into_iter().collect(),
            },
            _ => MapiObject::PendingMessage {
                folder_id,
                properties: property_values.into_iter().collect(),
                recipients: Vec::new(),
            },
        };
        let handle = session.allocate_output_handle(request.output_handle_index, pending_object);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
    }
}
