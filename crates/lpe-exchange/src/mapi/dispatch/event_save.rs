use super::*;

pub(super) async fn save_pending_event<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &mut MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    properties: HashMap<u32, MapiValue>,
    recipients: Vec<PendingRecipient>,
    recipients_modified: bool,
    fail_on_conflict: bool,
) {
    let imported_identity = match imported_event_identity_from_properties(&properties) {
        Ok(identity) => identity,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
    };
    let attachment_changes = session
        .pending_event_attachment_transactions
        .get(&handle)
        .cloned()
        .unwrap_or_default();
    let (collection_id, owner_email, owner_display_name) =
        match snapshot.collaboration_folder_for_id(folder_id) {
            Some(folder) => (
                folder.collection.id.clone(),
                folder.collection.owner_email.clone(),
                folder.collection.owner_display_name.clone(),
            ),
            None if folder_id == CALENDAR_FOLDER_ID => (
                DEFAULT_CALENDAR_COLLECTION_ID.to_string(),
                principal.email.clone(),
                principal.display_name.clone(),
            ),
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
    if let (Some(mut imported_identity), Some(uid)) = (
        imported_identity.clone(),
        imported_event_global_object_id(&properties),
    ) {
        if let Some(event) = snapshot.event_for_uid(folder_id, &uid).cloned() {
            // Outlook can upload an existing appointment under a remapped local
            // MID. The Global Object ID identifies the appointment; its durable
            // SourceKey remains the server's identity and CK/PCL still decide
            // whether this imported version may change canonical content.
            imported_identity.source_key = event.source_key.clone();
            let mut transaction =
                match imported_event_transaction(&event, imported_identity, fail_on_conflict) {
                    Ok(transaction) => transaction,
                    Err(error) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            error,
                        ));
                        return;
                    }
                };
            transaction.pending_properties = imported_event_content_properties(&properties);
            if recipients_modified {
                transaction.pending_recipients = Some(recipients);
            }
            save_existing_event(
                store,
                principal,
                session,
                handle_slots,
                request,
                snapshot,
                responses,
                handle,
                folder_id,
                event.id,
                transaction,
            )
            .await;
            return;
        }
    }
    let existing = default_event_for_mapping(principal.account_id, &collection_id);
    let mut input = match event_input_from_mapi(principal.account_id, None, &existing, &properties)
    {
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
    if recipients_modified {
        apply_calendar_pending_recipients(&mut input, &existing, &properties, &recipients);
    }
    if imported_identity.is_none() {
        materialize_owner_meeting_organizer(&mut input, &owner_email, &owner_display_name);
    }
    let imported_event = imported_identity.is_some();
    let custom_property_upserts =
        mapi_event_create_property_values_from_map(&properties, imported_event);
    let create_input = MapiEventCreateInput {
        principal_account_id: principal.account_id,
        collection_id,
        event: input,
        imported_identity,
        reminder: MapiEventReminderPatch {
            reminder_set,
            reminder_at,
            reminder_dismissed_at: None,
        },
        custom_property_upserts: custom_property_upserts.clone(),
        attachment_changes,
    };
    match store.create_mapi_event(create_input).await {
        Ok(MapiEventCreateOutcome::Created(created)) => {
            let event_id = created.mapi_object_id;
            let canonical_event_id = created.event.id;
            let version = created.version;
            snapshot.remember_created_event(
                folder_id,
                event_id,
                created.event,
                created.attachments,
            );
            snapshot.remember_event_version(version.clone());
            snapshot.remember_event_custom_property_changes(
                canonical_event_id,
                &custom_property_upserts,
                &[],
            );
            snapshot.remember_event_reminder_state(canonical_event_id, created.reminder);
            let disposition =
                save_disposition(request).expect("SaveFlags were validated before Event creation");
            remember_saved_event_handle(
                session,
                handle,
                folder_id,
                event_id,
                disposition,
                version.canonical_modseq,
            );
            clear_event_attachment_transaction(session, handle);
            session.record_notification(MapiNotificationEvent::content(folder_id, Some(event_id)));
            // [MS-OXCFXICS] sections 3.1.5.3 and 3.2.5.2.1: after the
            // imported Event is persisted, acknowledge its fresh server CN in
            // the content upload collector. Upload state never returns Given.
            record_sync_upload_content_change(
                session,
                folder_id,
                event_id,
                version.change_number,
                false,
                false,
            );
            append_save_changes_message_response(
                responses,
                handle_slots,
                request,
                handle,
                event_id,
            );
        }
        Ok(MapiEventCreateOutcome::NotFound) => responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        )),
        Ok(MapiEventCreateOutcome::AccessDenied) => responses.extend_from_slice(
            &rop_error_response(0x0C, request.response_handle_index(), 0x8007_0005),
        ),
        Err(_) => responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8000_4005,
        )),
    }
}

fn imported_event_global_object_id(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    properties
        .get(&PID_LID_GLOBAL_OBJECT_ID_TAG)
        .or_else(|| properties.get(&PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG))
        .and_then(|value| match value {
            MapiValue::Binary(value) => Some(format!(
                "mapi-goid:{}",
                lpe_domain::crypto::hex_lower(value)
            )),
            _ => None,
        })
}

fn imported_event_content_properties(
    properties: &HashMap<u32, MapiValue>,
) -> HashMap<u32, MapiValue> {
    properties
        .iter()
        .filter(|(tag, _)| {
            !matches!(
                **tag,
                PID_TAG_SOURCE_KEY
                    | PID_TAG_SEARCH_KEY
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
                    | PID_TAG_LAST_MODIFICATION_TIME
            )
        })
        .map(|(tag, value)| (*tag, value.clone()))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn save_existing_event<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &mut MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    event_id: u64,
    transaction: MapiEventTransaction,
) {
    let metric_flow = if transaction.imported_identity.is_some()
        || transaction.import_disposition != MapiEventImportDisposition::Apply
    {
        MapiCalendarEventSaveFlow::Ics
    } else {
        MapiCalendarEventSaveFlow::Direct
    };
    let attachment_changes = session
        .pending_event_attachment_transactions
        .get(&handle)
        .cloned()
        .unwrap_or_default();
    let Some(event) = snapshot.event_for_id(folder_id, event_id).cloned() else {
        record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let save_error = if !event.event.rights.may_write {
        Some(0x8007_0005)
    } else if !event_handle_is_writable(transaction.open_mode_flags, true) {
        // [MS-OXCMSG] section 3.2.5.3: saving a read-only Message object fails
        // with ecError and does not change the object or its open mode.
        Some(0x8000_4005)
    } else {
        None
    };
    if let Some(error) = save_error {
        record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            error,
        ));
        return;
    }
    let disposition =
        save_disposition(request).expect("SaveFlags were validated before Event save");
    let force_save = disposition == SaveDisposition::ForceSave;
    let commit_input = match staged_event_commit_input(
        principal,
        &event,
        &transaction,
        snapshot.reminder_for_source("calendar", event.canonical_id),
        force_save,
    ) {
        Ok(commit_input) => commit_input,
        Err(_) => {
            record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
    };
    let Some(mut commit_input) = commit_input else {
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Ics,
            MapiCalendarEventSaveOutcome::IcsIgnoredOlderOrSame,
        );
        clear_event_attachment_transaction(session, handle);
        remember_saved_event_handle(
            session,
            handle,
            folder_id,
            event_id,
            disposition,
            transaction.base_modseq,
        );
        append_save_changes_message_response(responses, handle_slots, request, handle, event_id);
        return;
    };
    if transaction.import_disposition == MapiEventImportDisposition::Apply {
        commit_input.attachment_changes = attachment_changes;
    }
    let event_input = commit_input.event.clone();
    let custom_property_upserts = commit_input.custom_property_upserts.clone();
    let custom_property_deletes = commit_input.custom_property_deletes.clone();
    match store.commit_mapi_event_update(commit_input).await {
        Ok(MapiEventCommitOutcome::Saved(saved)) => {
            let metric_outcome = match transaction.import_disposition {
                MapiEventImportDisposition::Apply
                    if matches!(metric_flow, MapiCalendarEventSaveFlow::Ics) =>
                {
                    MapiCalendarEventSaveOutcome::IcsApplied
                }
                MapiEventImportDisposition::Apply => MapiCalendarEventSaveOutcome::Committed,
                MapiEventImportDisposition::KeepServerContent => {
                    MapiCalendarEventSaveOutcome::IcsKeptServerContent
                }
                MapiEventImportDisposition::IgnoreOlderOrSame => unreachable!(),
            };
            record_mapi_calendar_event_save(metric_flow, metric_outcome);
            let updated_event = event_after_commit(event.event.clone(), event_input.as_ref());
            let version = saved.version;
            snapshot.remember_updated_event(
                folder_id,
                event_id,
                updated_event,
                version.clone(),
                saved.attachments,
            );
            snapshot.remember_event_custom_property_changes(
                event.canonical_id,
                &custom_property_upserts,
                &custom_property_deletes,
            );
            snapshot.remember_event_reminder_state(event.canonical_id, saved.reminder);
            remember_saved_event_handle(
                session,
                handle,
                folder_id,
                event_id,
                disposition,
                version.canonical_modseq,
            );
            clear_event_attachment_transaction(session, handle);
            session.record_notification(MapiNotificationEvent::content(folder_id, Some(event_id)));
            // [MS-OXCFXICS] sections 2.2.1.1.4 and 3.2.5.6: saving Event
            // content changes the normal CNSET, not the read-state CNSET.
            record_sync_upload_content_change(
                session,
                folder_id,
                event_id,
                version.change_number,
                false,
                false,
            );
            append_save_changes_message_response(
                responses,
                handle_slots,
                request,
                handle,
                event_id,
            );
        }
        Ok(MapiEventCommitOutcome::ObjectModified { .. }) => {
            record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_0109,
            ));
        }
        Ok(MapiEventCommitOutcome::NotFound) => {
            record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
        }
        Ok(MapiEventCommitOutcome::AccessDenied) => {
            record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8007_0005,
            ));
        }
        Err(_) => {
            record_mapi_calendar_event_save(metric_flow, MapiCalendarEventSaveOutcome::Failed);
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8000_4005,
            ));
        }
    }
}

fn remember_saved_event_handle(
    session: &mut MapiSession,
    handle: u32,
    folder_id: u64,
    event_id: u64,
    disposition: SaveDisposition,
    canonical_modseq: i64,
) {
    // The default disposition has no KeepOpen flag. Retain a read-only object
    // until this Execute finishes so a later ROP in the same buffer can read
    // the committed Message state ([MS-OXCFXICS] section 3.3.4.3.3.2.2.2).
    let open_mode_flags = event_open_mode_after_save(disposition).unwrap_or(0x00);
    session.handles.insert(
        handle,
        MapiObject::Event {
            folder_id,
            event_id,
            transaction: MapiEventTransaction::new(open_mode_flags, canonical_modseq),
        },
    );
}
