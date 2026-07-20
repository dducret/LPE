use super::*;
use lpe_storage::MapiContactImportedIdentity;

pub(super) fn imported_fai_identity(
    properties: &HashMap<u32, MapiValue>,
    imported_message_id: u64,
) -> Result<MapiFaiImportedIdentity> {
    let source_key = imported_message_source_key(properties)
        .ok_or_else(|| anyhow!("imported FAI SourceKey is missing or invalid"))?;
    if crate::mapi::identity::object_id_from_source_key(&source_key) != Some(imported_message_id) {
        return Err(anyhow!("imported FAI MID does not match its SourceKey"));
    }
    let source_counter = crate::mapi::identity::global_counter_from_store_id(imported_message_id)
        .ok_or_else(|| anyhow!("imported FAI MID is invalid"))?;
    if !(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
        ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
        .contains(&source_counter)
    {
        return Err(anyhow!("imported FAI MID is outside the dynamic range"));
    }
    let change_key = match properties.get(&PID_TAG_CHANGE_KEY) {
        Some(MapiValue::Binary(value)) => value.clone(),
        _ => return Err(anyhow!("imported FAI ChangeKey is missing or invalid")),
    };
    let predecessor_change_list = match properties.get(&PID_TAG_PREDECESSOR_CHANGE_LIST) {
        Some(MapiValue::Binary(value)) => value.clone(),
        _ => return Err(anyhow!("imported FAI PCL is missing or invalid")),
    };
    let last_modification_time = properties
        .get(&PID_TAG_LAST_MODIFICATION_TIME)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| anyhow!("imported FAI modification time is missing or invalid"))?;
    Ok(MapiFaiImportedIdentity {
        source_key,
        change_key,
        predecessor_change_list,
        last_modification_time,
    })
}

fn imported_contact_identity(
    properties: &HashMap<u32, MapiValue>,
    imported_message_id: u64,
) -> Result<MapiContactImportedIdentity> {
    // [MS-OXCFXICS] sections 2.2.3.2.4.2.1 and 3.3.5.8.7: the
    // Message returned by ImportMessageChange carries this identity quartet
    // until SaveChangesMessage publishes the new Contact.
    let identity = imported_fai_identity(properties, imported_message_id)?;
    Ok(MapiContactImportedIdentity {
        source_key: identity.source_key,
        change_key: identity.change_key,
        predecessor_change_list: identity.predecessor_change_list,
        last_modification_time: identity.last_modification_time,
    })
}

fn current_common_views_fai_identity(
    snapshot: &MapiMailStoreSnapshot,
    message_id: u64,
) -> Result<Option<MapiFaiImportedIdentity>> {
    if let Some(current) = snapshot.navigation_shortcut_message_for_id(message_id) {
        let identity = current
            .durable_identity
            .ok_or_else(|| anyhow!("durable Common Views WLink identity is missing"))?;
        return Ok(Some(MapiFaiImportedIdentity {
            source_key: identity.source_key,
            change_key: identity.change_key,
            predecessor_change_list: identity.predecessor_change_list,
            last_modification_time: identity.last_modification_time,
        }));
    }
    snapshot
        .associated_config_message_for_folder_and_source_key_id(COMMON_VIEWS_FOLDER_ID, message_id)
        .map(|message| {
            imported_fai_identity(
                &mapi_properties_from_json(&message.properties_json),
                message.id,
            )
        })
        .transpose()
}

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
        // [MS-OXCFXICS] sections 3.2.5.9.4.2 and 3.3.5.8.7: the
        // Message object returned by ImportMessageChange is populated by
        // subsequent ROPs and MUST NOT be persisted before SaveChangesMessage.
        let properties = property_values.into_iter().collect::<HashMap<_, _>>();
        let imported_identity = match imported_fai_identity(&properties, message_id) {
            Ok(identity) => identity,
            Err(_) => {
                responses.extend_from_slice(&rop_error_response(
                    0x72,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                return;
            }
        };
        let current_identity = match current_common_views_fai_identity(snapshot, message_id) {
            Ok(identity) => identity,
            Err(_) => {
                responses.extend_from_slice(&rop_error_response(
                    0x72,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
        };
        if let Some(current_identity) = current_identity {
            if current_identity.source_key != imported_identity.source_key {
                responses.extend_from_slice(&rop_error_response(
                    0x72,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            let relation = match sync_import_version_relation(
                &imported_identity.predecessor_change_list,
                &current_identity.predecessor_change_list,
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
            // [MS-OXCFXICS] sections 2.2.3.2.4.2.2 and 3.2.5.9.4.2:
            // FailOnConflict is reported by ImportMessageChange itself, before
            // a Message handle that could later be saved is returned.
            if relation == SyncImportVersionRelation::Conflict && import_flag & 0x40 != 0 {
                responses.extend_from_slice(&rop_error_response(
                    0x72,
                    request.response_handle_index(),
                    0x8004_0802,
                ));
                return;
            }
        }
        let pending_object = MapiObject::PendingNavigationShortcut {
            folder_id,
            properties,
            imported_message_id: Some(message_id),
            fail_on_conflict: import_flag & 0x40 != 0,
        };
        let handle = session.allocate_output_handle(request.output_handle_index, pending_object);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x72",
            folder_id = format_args!("0x{:016x}", folder_id),
            imported_message_id = format_args!("0x{message_id:016x}"),
            "rca debug mapi staged common views navigation shortcut import"
        );
        responses.extend_from_slice(&rop_synchronization_import_message_change_response(
            &request,
        ));
        output_handles.push(handle);
        return;
    }
    if import_flag & 0x10 != 0 {
        let pending_object = MapiObject::PendingAssociatedMessage {
            folder_id,
            properties: property_values.into_iter().collect(),
            imported_message_id: Some(message_id),
            fail_on_conflict: import_flag & 0x40 != 0,
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
            Some(MapiCollaborationFolderKind::Contacts) => {
                let properties = property_values.into_iter().collect::<HashMap<_, _>>();
                let imported_identity = match imported_contact_identity(&properties, message_id) {
                    Ok(identity) => identity,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        return;
                    }
                };
                MapiObject::PendingContact {
                    folder_id,
                    properties,
                    imported_identity: Some(imported_identity),
                    fail_on_conflict: import_flag & 0x40 != 0,
                }
            }
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
