use super::properties::*;
use super::rop::*;
use super::session::*;
use super::store_adapter::*;
use super::sync::*;
use super::tables::*;
use super::transport::*;
use super::*;

pub(in crate::mapi) async fn execute_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
    V: Detector,
{
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return execute_failure_response(request_id, 13, "missing MAPI session cookie", None);
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return execute_failure_response(
            request_id,
            6,
            "invalid MAPI request sequence cookie",
            None,
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return execute_failure_response(
            request_id,
            15,
            "MAPI session already has an active request",
            None,
        );
    };
    let Some(session) = get_session(&session_id) else {
        return execute_failure_response(request_id, 10, "MAPI session context not found", None);
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            None,
        );
    }

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("invalid Execute request body: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let Some(mut session) = remove_session(&session_id) else {
        return execute_failure_response(
            request_id,
            10,
            "MAPI session context not found",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }
    let rop_fingerprint = mapi_payload_fingerprint(&execute.rop_buffer);
    if let Some(cached) = session.completed_execute_requests.get(request_id).cloned() {
        if cached.rop_fingerprint == rop_fingerprint {
            store_session(session_id.clone(), session);
            return mapi_response(
                "Execute",
                request_id,
                0,
                cached.response_body,
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
        store_session(session_id.clone(), session);
        return execute_failure_response(
            request_id,
            12,
            "reused MAPI Execute request id with a different ROP payload",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }

    let access_plan = plan_mapi_store_access(&session, &execute.rop_buffer);
    let snapshot =
        match load_mapi_store_for_access_plan(store, principal.account_id, &access_plan, 500).await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                store_session(session_id.clone(), session);
                return execute_failure_response(
                    request_id,
                    4,
                    &format!("failed to load MAPI mail store view: {error}"),
                    Some(session_cookie(endpoint, &session_id, false)),
                );
            }
        };
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    let rop_buffer = execute_rops(
        store,
        principal,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        validator,
        &execute.rop_buffer,
    )
    .await;
    let response_body = execute_success_body(rop_buffer, Vec::new());
    cache_execute_response(&mut session, request_id, rop_fingerprint, &response_body);
    store_session(session_id.clone(), session);
    mapi_response(
        "Execute",
        request_id,
        0,
        response_body,
        Some(session_cookie(endpoint, &session_id, false)),
    )
}

pub(in crate::mapi) struct ExecuteRequest {
    rop_buffer: Vec<u8>,
}

pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let _flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let _max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest { rop_buffer })
}

pub(in crate::mapi) async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    rop_buffer: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(unsupported_rop_response(0, 0), &[]);
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    let mut handle_slots = read_handle_table(handle_table);

    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    let mut echo_input_handle_table = false;
    while cursor.remaining() > 0 {
        let request = match read_rop_request(&mut cursor) {
            Ok(request) => request,
            Err(_) => {
                responses.extend_from_slice(&unsupported_rop_response(0, 0));
                break;
            }
        };
        match request.rop_id {
            0x01 => release_handle_slot(session, &mut handle_slots, &request),
            0x02 => {
                let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::Folder { folder_id },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_folder_response(&request));
                output_handles.push(handle);
            }
            0x03 => {
                let folder_id = request.folder_id().unwrap_or(INBOX_FOLDER_ID);
                let message_id = request.message_id().unwrap_or(0);
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Contact {
                            folder_id,
                            contact_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &contact.contact.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &event.event.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x03,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            0x04 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(ROOT_FOLDER_ID);
                let columns = default_hierarchy_columns();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::HierarchyTable {
                        folder_id,
                        columns,
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_hierarchy_table_response(
                    &request,
                    hierarchy_row_count(folder_id, mailboxes, snapshot),
                ));
                output_handles.push(handle);
            }
            0x05 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(INBOX_FOLDER_ID);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::ContentsTable {
                        folder_id,
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_contents_table_response(
                    &request,
                    folder_message_count(folder_id, mailboxes, emails, snapshot),
                ));
                output_handles.push(handle);
            }
            0x06 => {
                let folder_id = request.folder_id().unwrap_or_else(|| {
                    input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                });
                if snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && folder_row_for_id(folder_id, mailboxes).is_none()
                    && !matches!(
                        folder_id,
                        INBOX_FOLDER_ID
                            | DRAFTS_FOLDER_ID
                            | SENT_FOLDER_ID
                            | TRASH_FOLDER_ID
                            | OUTBOX_FOLDER_ID
                    )
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let pending_object = match snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ => MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    },
                };
                let handle =
                    session.allocate_output_handle(request.output_handle_index, pending_object);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_message_response(&request));
                output_handles.push(handle);
            }
            0x07 => {
                echo_input_handle_table = true;
                responses.extend_from_slice(&rop_get_properties_specific_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                ));
            }
            0x08 => responses.extend_from_slice(&rop_get_properties_all_response(
                &request,
                input_object(session, &handle_slots, &request),
                principal,
                mailboxes,
                emails,
                snapshot,
            )),
            0x09 => responses.extend_from_slice(&rop_get_properties_list_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            0x0A | 0x79 => {
                echo_input_handle_table = true;
                let set_result = match request.property_values() {
                    Ok(values) => match input_object(session, &handle_slots, &request).cloned() {
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                        }) => {
                            apply_canonical_message_property_values(
                                store, principal, folder_id, message_id, values, mailboxes, emails,
                            )
                            .await
                        }
                        Some(MapiObject::Contact {
                            folder_id,
                            contact_id,
                        }) => {
                            apply_canonical_contact_property_values(
                                store, principal, folder_id, contact_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => {
                            apply_canonical_event_property_values(
                                store, principal, folder_id, event_id, values, snapshot,
                            )
                            .await
                        }
                        _ => apply_mapi_property_values(
                            input_object_mut(session, &handle_slots, &request),
                            values,
                        ),
                    },
                    Err(error) => Err(error),
                };
                match set_result {
                    Ok(()) => responses.extend_from_slice(&rop_set_properties_response(&request)),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x0B | 0x7A => {
                let deleted = delete_mapi_properties(
                    input_object_mut(session, &handle_slots, &request),
                    &request.property_tags(),
                );
                match deleted {
                    Ok(()) => {
                        responses.extend_from_slice(&rop_delete_properties_response(&request))
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x0C => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match session.handles.get(&handle).cloned() {
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
                            continue;
                        };
                        let input = contact_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_contact_for_mapping(
                                principal.account_id,
                                &folder.collection.id,
                            ),
                            &properties,
                        );
                        match store
                            .create_accessible_contact(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(contact) => {
                                let contact_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Contact,
                                    contact.id,
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
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Contact {
                                        folder_id,
                                        contact_id,
                                    },
                                );
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, contact_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingEvent {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = match event_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_event_for_mapping(principal.account_id, &folder.collection.id),
                            &properties,
                        ) {
                            Ok(input) => input,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        match store
                            .create_accessible_event(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(event) => {
                                let event_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::CalendarEvent,
                                    event.id,
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
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Event {
                                        folder_id,
                                        event_id,
                                    },
                                );
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, event_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::Contact { contact_id, .. })
                    | Some(MapiObject::Event {
                        event_id: contact_id,
                        ..
                    }) => {
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, contact_id,
                        ));
                        continue;
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
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let input =
                    jmap_import_from_pending_message(principal, mailbox, &properties, &recipients);
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
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            email.id,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                            },
                        );
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, message_id,
                        ));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x0D => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::PendingMessage { recipients, .. }) => {
                    recipients.clear();
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x0D,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x0E => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::PendingMessage { recipients, .. }) => {
                    match request.modify_recipients() {
                        Ok(changes) => {
                            apply_pending_recipient_changes(recipients, changes);
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x0E,
                            request.response_handle_index(),
                            0x8004_0102,
                        )),
                    }
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x0E,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x0F => responses.extend_from_slice(&rop_read_recipients_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x10 => responses.extend_from_slice(&rop_reload_cached_information_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x11 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let unread = unread_from_read_flags(request.read_flags());
                let changed = unread.is_some_and(|unread| unread != email.unread);
                if let Some(unread) = unread {
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-set-message-read-flag".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                }
                responses.extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
            }
            0x12 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable { columns, .. })
                | Some(MapiObject::ContentsTable { columns, .. })
                | Some(MapiObject::AttachmentTable { columns, .. }) => {
                    *columns = request.property_tags();
                    responses.extend_from_slice(&rop_set_columns_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x13 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                }) => {
                    *sort_orders = request.sort_orders();
                    *position = 0;
                    bookmarks.clear();
                    responses.extend_from_slice(&rop_sort_table_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x13,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x14 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                }) => match request.restriction() {
                    Ok(parsed) => {
                        *restriction = parsed;
                        *position = 0;
                        bookmarks.clear();
                        responses.extend_from_slice(&rop_restrict_response(&request));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                },
                _ => responses.extend_from_slice(&rop_error_response(
                    0x14,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x15 => responses.extend_from_slice(&rop_query_rows_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x16 => responses.extend_from_slice(&rop_get_status_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            0x17 => responses.extend_from_slice(&rop_query_position_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x18 => responses.extend_from_slice(&rop_seek_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x19 => responses.extend_from_slice(&rop_seek_row_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x1A => responses.extend_from_slice(&rop_seek_row_fractional_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x1B => responses.extend_from_slice(&rop_create_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
            0x37 => responses.extend_from_slice(&rop_query_columns_all_response(
                &request,
                input_object(session, &handle_slots, &request),
                snapshot,
            )),
            0x1C => {
                let parent_folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                if !is_root_hierarchy_folder(parent_folder_id)
                    && folder_row_for_id(parent_folder_id, mailboxes).is_none()
                    && role_for_folder_id(parent_folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let display_name = request.create_folder_display_name();
                let display_name = display_name.trim();
                if display_name.is_empty() || request.create_folder_type() == 0 {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }

                if request.create_folder_open_existing() {
                    if let Some(existing) = mailboxes
                        .iter()
                        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
                    {
                        let folder_id = mapi_folder_id(existing);
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder { folder_id },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, true,
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.to_string(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-create-folder".to_string(),
                            subject: display_name.to_string(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder { folder_id },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        output_handles.push(handle);
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    )),
                }
            }
            0x1D => {
                let Some(_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(folder_id) = request.delete_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if mailbox.role != "custom" {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }

                let partial_completion = store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-folder".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                    .is_err();
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x1E | 0x91 => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_contact(principal.account_id, contact.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_event(principal.account_id, event.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.rop_id == 0x91 || email.mailbox_role == "trash" {
                        store
                            .delete_jmap_email(
                                principal.account_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message-to-trash".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email(
                                principal.account_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
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
                    request.rop_id,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x1F | 0x20 => {
                let folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x20,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let message_id = request.status_message_id().unwrap_or(0);
                if message_for_id(folder_id, message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x20,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let key = (folder_id, message_id);
                let old_status = session.message_statuses.get(&key).copied().unwrap_or(0);
                if request.rop_id == 0x20 {
                    let mask = request.message_status_mask();
                    let new_status = (old_status & !mask) | (request.message_status_flags() & mask);
                    if new_status == 0 {
                        session.message_statuses.remove(&key);
                    } else {
                        session.message_statuses.insert(key, new_status);
                    }
                }
                responses.extend_from_slice(&rop_message_status_response(&request, old_status));
            }
            0x4F => responses.extend_from_slice(&rop_find_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x52 => responses.extend_from_slice(&rop_get_valid_attachments_response(
                &request,
                input_object(session, &handle_slots, &request),
                snapshot,
            )),
            0x21 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x21,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let row_count = snapshot
                    .attachments_for_message(*folder_id, *message_id)
                    .unwrap_or_default()
                    .len() as u32;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentTable {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_get_attachment_table_response(&request, row_count));
                output_handles.push(handle);
            }
            0x22 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                if snapshot
                    .attachment_for_message(*folder_id, *message_id, attach_num)
                    .is_some()
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Attachment {
                            folder_id: *folder_id,
                            message_id: *message_id,
                            attach_num,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_attachment_response(&request));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            0x23 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x0000_04B9,
                    ));
                    continue;
                };
                if message_for_id(*folder_id, *message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let attach_num =
                    next_pending_attachment_num(session, *folder_id, *message_id, snapshot);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingAttachment {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        attach_num,
                        properties: HashMap::new(),
                        data: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_attachment_response(&request, attach_num));
                output_handles.push(handle);
            }
            0x24 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                let Some(attachment) =
                    snapshot.attachment_for_message(*folder_id, *message_id, attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
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
                        responses.extend_from_slice(&rop_simple_success_response(&request))
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x25 => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(MapiObject::PendingAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    properties,
                    data,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attachment = pending_attachment_upload(attach_num, &properties, data);
                let validation = validator.validate_bytes(
                    ValidationRequest {
                        ingress_context: IngressContext::ExchangeAttachment,
                        declared_mime: Some(attachment.media_type.clone()),
                        filename: Some(attachment.file_name.clone()),
                        expected_kind: mapi_expected_attachment_kind(
                            &attachment.media_type,
                            &attachment.file_name,
                        ),
                    },
                    &attachment.blob_bytes,
                );
                let Ok(outcome) = validation else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if outcome.policy_decision != PolicyDecision::Accept {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let mut attachment = attachment;
                if attachment.media_type == "application/octet-stream"
                    && !outcome.detected_mime.trim().is_empty()
                {
                    attachment.media_type = outcome.detected_mime;
                }
                match store
                    .add_message_attachment(
                        principal.account_id,
                        email.id,
                        attachment,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-attachment".to_string(),
                            subject: format!("message:{}", email.id),
                        },
                    )
                    .await
                {
                    Ok(Some((_email, stored))) => {
                        session.handles.insert(
                            handle,
                            MapiObject::SavedAttachment {
                                folder_id,
                                message_id,
                                attach_num,
                                file_reference: stored.file_reference,
                                file_name: stored.file_name,
                                media_type: stored.media_type,
                                size_octets: stored.size_octets,
                            },
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x2B => {
                let Some(input_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((stream_data, writable_target)) = open_stream_data(
                    store,
                    principal,
                    session,
                    input_handle,
                    request.stream_property_tag().unwrap_or(0),
                    request.stream_open_mode().unwrap_or(0),
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = stream_data.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: stream_data,
                        position: 0,
                        writable_target,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_stream_response(&request, stream_size));
                output_handles.push(handle);
            }
            0x2C => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_read_stream_response(&request, stream));
            }
            0x2E => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_seek_stream_response(&request, stream));
            }
            0x2F => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match set_attachment_stream_size(
                    session,
                    stream_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some(()) => responses.extend_from_slice(&rop_simple_success_response(&request)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x2D | 0x90 | 0xA3 => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match write_stream(session, stream_handle, request.stream_write_data()) {
                    Some(written) => {
                        responses.extend_from_slice(&rop_write_stream_response(&request, written))
                    }
                    None => {
                        let error_code = stream_write_error_code(
                            stream_write_error(session, stream_handle)
                                .unwrap_or(StreamWriteError::NotFound),
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            error_code,
                        ))
                    }
                }
            }
            0x3A => {
                let Some(source_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match copy_stream(
                    session,
                    source_handle,
                    destination_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some((read, written)) => responses
                        .extend_from_slice(&rop_copy_to_stream_response(&request, read, written)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x5E => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { data, .. }) => {
                    responses.extend_from_slice(&rop_get_stream_size_response(&request, data.len()))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5E,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            0x3B => match input_object(session, &handle_slots, &request).cloned() {
                Some(MapiObject::AttachmentStream {
                    data,
                    position,
                    writable_target: None,
                }) => {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::AttachmentStream {
                            data,
                            position,
                            writable_target: None,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                    output_handles.push(handle);
                }
                Some(MapiObject::AttachmentStream { .. }) => responses.extend_from_slice(
                    &rop_error_response(0x3B, request.response_handle_index(), 0x8004_0102),
                ),
                _ => responses.extend_from_slice(&rop_error_response(
                    0x3B,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            0x5B | 0x5C => responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8004_0102,
            )),
            0x5D => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5D,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            0x32 | 0x4A => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(object) = session.handles.get(&handle).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let input = match object {
                    MapiObject::PendingMessage {
                        properties,
                        recipients,
                        ..
                    } => mapi_submit_from_pending_message(principal, &properties, &recipients),
                    MapiObject::Message {
                        folder_id,
                        message_id,
                    } => {
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        else {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        if email.mailbox_role != "drafts" {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        let protected_emails = match store
                            .fetch_jmap_emails_with_protected_bcc(principal.account_id, &[email.id])
                            .await
                        {
                            Ok(emails) => emails,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to load protected Bcc recipients for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        let protected_email =
                            protected_emails.iter().find(|loaded| loaded.id == email.id);
                        mapi_submit_from_email(principal, protected_email.unwrap_or(email))
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                match store
                    .submit_message(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-submit-message".to_string(),
                            subject: format!("handle:{handle}"),
                        },
                    )
                    .await
                {
                    Ok(submitted) => {
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            submitted.message_id,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id: submitted_mapi_folder_id(&submitted, mailboxes),
                                message_id,
                            },
                        );
                        if request.rop_id == 0x4A {
                            responses
                                .extend_from_slice(&rop_transport_send_success_response(&request));
                        } else {
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x33 => {
                let source_folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let target_folder_id = match request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    }) {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.move_copy_message_ids() {
                    let Some(email) =
                        message_for_id(source_folder_id, message_id, mailboxes, emails)
                    else {
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
            0x26 => responses.extend_from_slice(&rop_error_response(
                0x26,
                request.response_handle_index(),
                0x8004_0102,
            )),
            0x27 => {
                echo_input_handle_table = true;
                let Some(message_class) = request.receive_folder_message_class() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                if !valid_receive_folder_message_class(message_class) {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_get_receive_folder_response(
                    &request,
                    explicit_receive_folder_message_class(message_class),
                ));
            }
            0x66 => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x66,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let unread = unread_from_read_flags(request.read_flags());
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if let Some(unread) = unread {
                        if store
                            .update_jmap_email_flags(
                                principal.account_id,
                                email.id,
                                Some(unread),
                                None,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-set-read-flags".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                }
                responses
                    .extend_from_slice(&rop_set_read_flags_response(&request, partial_completion));
            }
            0x70 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let sync_type = request.sync_type();
                let sync_mailboxes = sync_mailboxes_for(folder_id, sync_type, mailboxes);
                let sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
                let state = mapi_mailstore::sync_state_token_with_attachments(
                    &sync_mailboxes,
                    &sync_emails,
                    &sync_attachment_facts,
                );
                let transfer_buffer = mapi_mailstore::sync_manifest_buffer_with_attachments(
                    folder_id,
                    &sync_mailboxes,
                    &sync_emails,
                    &sync_attachment_facts,
                );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type,
                        state,
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_configure_response(&request));
                output_handles.push(handle);
            }
            0x4B => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x4B,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let requested_ids = request.fast_transfer_message_ids();
                let mut selected = emails_for_folder(folder_id, mailboxes, emails)
                    .into_iter()
                    .filter(|email| requested_ids.contains(&mapi_message_id(email)))
                    .cloned()
                    .collect::<Vec<_>>();
                selected.sort_by(|left, right| left.id.cmp(&right.id));
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &selected, snapshot);
                let transfer_buffer =
                    mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                        folder_id,
                        &[],
                        &selected,
                        &sync_attachment_facts,
                    );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            0x4C | 0x4D | 0x69 => {
                let Some(object) = input_object(session, &handle_slots, &request).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((folder_id, transfer_buffer)) =
                    fast_transfer_manifest_for_object(&object, mailboxes, emails, snapshot)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            0x4E => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    sync_type,
                    transfer_buffer,
                    transfer_position,
                    ..
                }) => {
                    let _ = *sync_type;
                    responses.extend_from_slice(&rop_fast_transfer_source_get_buffer_response(
                        &request,
                        transfer_buffer,
                        transfer_position,
                    ));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x4E,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x86 => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource { .. })
                | Some(MapiObject::SynchronizationCollector { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x86,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x75 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state_upload_buffer,
                    ..
                })
                | Some(MapiObject::SynchronizationCollector {
                    state_upload_buffer,
                    ..
                }) => {
                    state_upload_buffer.clear();
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x75,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x76 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state_upload_buffer,
                    ..
                })
                | Some(MapiObject::SynchronizationCollector {
                    state_upload_buffer,
                    ..
                }) => {
                    state_upload_buffer.extend_from_slice(request.stream_data());
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x76,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x77 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state,
                    state_upload_buffer,
                    sync_type,
                    transfer_buffer,
                    transfer_position,
                    ..
                }) => {
                    if !state_upload_buffer.is_empty() {
                        *state = std::mem::take(state_upload_buffer);
                    }
                    *transfer_buffer = final_incremental_sync_stream(*sync_type);
                    *transfer_position = 0;
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                Some(MapiObject::SynchronizationCollector {
                    state,
                    state_upload_buffer,
                    ..
                }) => {
                    if !state_upload_buffer.is_empty() {
                        *state = std::mem::take(state_upload_buffer);
                    }
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x77,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x7E => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x7E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationCollector {
                        folder_id,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            0x82 => {
                let Some((folder_id, state)) =
                    synchronization_context_state(input_object(session, &handle_slots, &request))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x82,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let transfer_buffer = if state.is_empty() {
                    let sync_mailboxes = sync_mailboxes_for(folder_id, 0x01, mailboxes);
                    let sync_emails = sync_emails_for(folder_id, 0x01, mailboxes, emails);
                    let sync_attachment_facts =
                        sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
                    mapi_mailstore::sync_state_token_with_attachments(
                        &sync_mailboxes,
                        &sync_emails,
                        &sync_attachment_facts,
                    )
                } else {
                    state
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type: 0,
                        state: transfer_buffer.clone(),
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_synchronization_get_transfer_state_response(&request));
                output_handles.push(handle);
            }
            0x72 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match request.import_property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let message_id = request.import_message_id().unwrap_or(0);
                if message_id != 0
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_some()
                {
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
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, message_id),
                    );
                    output_handles.push(handle);
                } else {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::PendingMessage {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                            recipients: Vec::new(),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, 0),
                    );
                    output_handles.push(handle);
                }
            }
            0x73 => {
                let Some(_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
                let Some(display_name) = display_name else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if system_folder_display_name(&display_name) {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if let Some(existing) =
                    imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
                {
                    if existing.role == "custom"
                        && existing.name.eq_ignore_ascii_case(&display_name)
                    {
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(
                                &request,
                                mapi_folder_id(existing),
                            ),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                    }
                    continue;
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.clone(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-hierarchy-change".to_string(),
                            subject: display_name.clone(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x73,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(
                                &request, folder_id,
                            ),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x74 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x74,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.import_delete_message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .delete_jmap_email(
                            principal.account_id,
                            email.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-delete".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x74,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x78 => {
                let Some((message_id, target_folder_id)) = request.import_move() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let source_folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .move_jmap_email(
                        principal.account_id,
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
                        let moved_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            moved.id,
                            None,
                        )
                        .await
                        {
                            Ok(moved_id) => moved_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x78,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request, moved_id),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x80 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let mut partial_completion = false;
                for (message_id, unread) in request.import_read_state_changes() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-read-state".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x80,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x93 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource { state, .. })
                | Some(MapiObject::SynchronizationCollector { state, .. }) => {
                    state.extend_from_slice(request.local_replica_midset_deleted());
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x93,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x7F => {
                echo_input_handle_table = true;
                let (first_global_counter, count) = mapi_mailstore::local_replica_id_range(
                    principal.account_id,
                    request.local_replica_id_count(),
                    session.next_local_replica_sequence,
                );
                session.next_local_replica_sequence =
                    session.next_local_replica_sequence.saturating_add(1).max(1);
                responses.extend_from_slice(&rop_get_local_replica_ids_response(
                    &request,
                    first_global_counter,
                    count,
                ));
            }
            0x59 | 0x5A => responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8004_0102,
            )),
            0x6D => responses.extend_from_slice(&rop_get_transport_folder_response(&request)),
            0x6F => responses.extend_from_slice(&rop_options_data_response(&request)),
            0x68 => responses.extend_from_slice(&rop_get_receive_folder_table_response(&request)),
            0x43 => responses.extend_from_slice(&rop_long_term_id_from_id_response(&request)),
            0x44 => responses.extend_from_slice(&rop_id_from_long_term_id_response(&request)),
            0x45 => responses.extend_from_slice(&rop_public_folder_is_ghosted_response(&request)),
            0x49 => responses.extend_from_slice(&rop_get_address_types_response(&request)),
            0x55 => responses
                .extend_from_slice(&rop_get_names_from_property_ids_response(&request, session)),
            0x56 => {
                echo_input_handle_table = true;
                let properties = match request.named_property_names() {
                    Ok(properties) => properties,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x56,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                if properties.is_empty()
                    && matches!(
                        input_object(session, &handle_slots, &request),
                        Some(MapiObject::Logon)
                    )
                {
                    let property_ids = session
                        .named_properties_for_query(None)
                        .into_iter()
                        .map(|(property_id, _property)| property_id)
                        .collect::<Vec<_>>();
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                    continue;
                }
                let mut property_ids = Vec::with_capacity(properties.len());
                let mut exhausted = false;
                for property in properties {
                    match session.property_id_for_name(property, request.named_property_create()) {
                        Some(property_id) => property_ids.push(property_id),
                        None if request.named_property_create() => {
                            exhausted = true;
                            break;
                        }
                        None => property_ids.push(0),
                    }
                }
                if exhausted {
                    responses.extend_from_slice(&rop_error_response(
                        0x56,
                        request.response_handle_index(),
                        0x8007_000E,
                    ));
                } else {
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                }
            }
            0x5F => {
                responses.extend_from_slice(&rop_query_named_properties_response(&request, session))
            }
            0x7B => responses.extend_from_slice(&rop_get_store_state_response(&request)),
            0x81 => {
                if input_object_mut(session, &handle_slots, &request)
                    .is_some_and(reset_table_position)
                {
                    responses.extend_from_slice(&rop_reset_table_response(&request));
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x81,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                }
            }
            0x89 => responses.extend_from_slice(&rop_free_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
            0xFE => {
                let handle =
                    session.allocate_output_handle(request.output_handle_index, MapiObject::Logon);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_logon_response_body(principal, &request));
                output_handles.push(handle);
            }
            rop_id => responses.extend_from_slice(&unsupported_rop_response(
                rop_id,
                request.response_handle_index(),
            )),
        }
    }
    let response_handles =
        response_handle_table(&handle_slots, &output_handles, echo_input_handle_table);
    let response = if extended {
        rop_buffer_with_response_spec(responses, &response_handles)
    } else {
        rop_buffer_with_response(responses, &response_handles)
    };
    if extended {
        rpc_header_ext_rop_buffer(response)
    } else {
        response
    }
}

async fn remember_created_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
) -> Result<u64>
where
    S: ExchangeStore,
{
    let requests = [MapiIdentityRequest {
        object_kind,
        canonical_id,
        reserved_global_counter,
    }];
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    let object_id = records
        .first()
        .map(|record| record.object_id)
        .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
    crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
    Ok(object_id)
}
