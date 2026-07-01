use super::*;

pub(super) async fn append_create_folder_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let parent_folder_id =
        match input_object(session, handle_slots, request).and_then(MapiObject::folder_id) {
            Some(folder_id) => folder_id,
            None => {
                responses.extend_from_slice(&rop_error_response(
                    0x1C,
                    request.output_handle_index.unwrap_or(0),
                    0x0000_04B9,
                ));
                return;
            }
        };
    let parent_mailbox = folder_row_for_id(parent_folder_id, mailboxes);
    let parent_public_folder_id = snapshot
        .public_folder_for_id(parent_folder_id)
        .map(|folder| folder.folder.id);
    if !is_root_hierarchy_folder(parent_folder_id)
        && parent_mailbox.is_none()
        && parent_public_folder_id.is_none()
        && parent_folder_id != SEARCH_FOLDER_ID
        && role_for_folder_id(parent_folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x1C,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }

    let create_parent_id = parent_mailbox.map(|mailbox| mailbox.id);
    let display_name = request.create_folder_display_name();
    let display_name = display_name.trim();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x1c",
        parent_folder_id = %format!("{parent_folder_id:#018x}"),
        folder_type = request.create_folder_type(),
        open_existing = request.create_folder_open_existing(),
        display_name = display_name,
        message = "rca debug mapi create folder request",
    );
    if display_name.is_empty()
        || !matches!(request.create_folder_type(), 1 | 2)
        || request.create_folder_reserved() != 0
    {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x1c",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_type = request.create_folder_type(),
            open_existing = request.create_folder_open_existing(),
            reserved = request.create_folder_reserved(),
            display_name = display_name,
            response_error = "0x80070057",
            message = "rca debug mapi create folder invalid request",
        );
        responses.extend_from_slice(&rop_error_response(
            0x1C,
            request.output_handle_index.unwrap_or(0),
            0x8007_0057,
        ));
        return;
    }

    if let Some(folder_id) = advertised_special_folder_id_for_create(parent_folder_id, display_name)
    {
        if session.advertised_special_folder_was_deleted(folder_id) {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x1c",
                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                folder_type = request.create_folder_type(),
                open_existing = request.create_folder_open_existing(),
                display_name = display_name,
                deleted_advertised_folder_id = %format!("0x{folder_id:016x}"),
                message = "rca debug mapi create folder skipped deleted advertised special folder",
            );
        } else {
            let requested_open_existing = request.create_folder_open_existing();
            let response_existing = private_create_folder_is_existing_response_flag();
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x1c",
                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                folder_type = request.create_folder_type(),
                open_existing = requested_open_existing,
                display_name = display_name,
                matched_advertised_folder_id = %format!("0x{folder_id:016x}"),
                response_existing_folder = response_existing,
                message = "rca debug mapi create folder opened advertised special folder",
            );
            let properties = folder_properties_for_open(
                store, principal, session, folder_id, mailboxes, snapshot,
            )
            .await;
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_create_folder_response(
                request,
                folder_id,
                response_existing,
            ));
            if !requested_open_existing {
                session.record_notification(MapiNotificationEvent::hierarchy(
                    parent_folder_id,
                    Some(folder_id),
                ));
            }
            output_handles.push(handle);
            return;
        }
    }

    if let Some(parent_public_folder_id) = parent_public_folder_id {
        if request.create_folder_type() != 1 {
            responses.extend_from_slice(&rop_error_response(
                0x1C,
                request.output_handle_index.unwrap_or(0),
                0x8000_4005,
            ));
            return;
        }
        let existing_public_folder_id = snapshot
            .public_folders()
            .iter()
            .find(|folder| {
                folder.folder.parent_folder_id == Some(parent_public_folder_id)
                    && folder.folder.lifecycle_state == "active"
                    && folder
                        .folder
                        .display_name
                        .eq_ignore_ascii_case(display_name)
            })
            .map(|folder| folder.folder.id);
        if let Some(existing_public_folder_id) = existing_public_folder_id {
            if !request.create_folder_open_existing() {
                responses.extend_from_slice(&rop_error_response(
                    0x1C,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_0604,
                ));
                return;
            }
            let folder_id = match remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::PublicFolder,
                existing_public_folder_id,
                None,
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
                    return;
                }
            };
            let properties = folder_properties_for_open(
                store, principal, session, folder_id, mailboxes, snapshot,
            )
            .await;
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_create_folder_response(request, folder_id, false));
            output_handles.push(handle);
            return;
        }

        match store
            .create_public_folder_child(
                CreatePublicFolderInput {
                    account_id: principal.account_id,
                    parent_folder_id: parent_public_folder_id,
                    display_name: display_name.to_string(),
                    folder_class: "IPF.Note".to_string(),
                    sort_order: 0,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-create-public-folder".to_string(),
                    subject: display_name.to_string(),
                },
            )
            .await
        {
            Ok(folder) => {
                let folder_id = match remember_created_mapi_identity(
                    store,
                    principal,
                    MapiIdentityObjectKind::PublicFolder,
                    folder.id,
                    None,
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
                        return;
                    }
                };
                let properties = public_folder_handle_properties(&folder, folder_id);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::Folder {
                        folder_id,
                        properties,
                    },
                );
                set_handle_slot(handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_folder_response(request, folder_id, false));
                session.record_notification(MapiNotificationEvent::hierarchy(
                    parent_folder_id,
                    Some(folder_id),
                ));
                output_handles.push(handle);
            }
            Err(_) => responses.extend_from_slice(&rop_error_response(
                0x1C,
                request.output_handle_index.unwrap_or(0),
                0x8007_0005,
            )),
        }
        return;
    }

    if parent_folder_id == SEARCH_FOLDER_ID || request.create_folder_type() == FOLDER_SEARCH as u8 {
        if let Some(definition) =
            snapshot.user_saved_search_folder_definition_by_display_name(display_name, "message")
        {
            let folder_id = match remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::SearchFolderDefinition,
                definition.id,
                None,
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
                    return;
                }
            };
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x1c",
                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                folder_id = %format!("{folder_id:#018x}"),
                search_folder_id = %definition.id,
                folder_type = request.create_folder_type(),
                open_existing = request.create_folder_open_existing(),
                display_name = display_name,
                reused_existing_search_folder = true,
                message = "rca debug mapi create folder reused search folder",
            );
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties: search_folder_handle_properties(
                        definition,
                        folder_id,
                        principal.account_id,
                    ),
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_create_folder_response(
                request,
                folder_id,
                private_create_folder_is_existing_response_flag(),
            ));
            output_handles.push(handle);
            return;
        }
        let definition_id = Uuid::new_v4();
        let folder_id = match remember_created_mapi_identity(
            store,
            principal,
            MapiIdentityObjectKind::SearchFolderDefinition,
            definition_id,
            None,
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
                return;
            }
        };
        let definition = SearchFolderDefinition {
            id: definition_id,
            account_id: principal.account_id,
            role: "custom".to_string(),
            display_name: display_name.to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: json!({
                "kind": "mapi_bounded",
                "scope": "folders",
                "recursive": true,
                "folderIds": [],
                "folderRoles": ["inbox"]
            }),
            restriction_json: json!({
                "kind": "mapi_bounded",
                "all": []
            }),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        };
        session.remember_search_folder_definition(folder_id, definition.clone());
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x1c",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_id = %format!("{folder_id:#018x}"),
            search_folder_id = %definition.id,
            folder_type = request.create_folder_type(),
            open_existing = request.create_folder_open_existing(),
            display_name = display_name,
            message = "rca debug mapi create folder staged search folder",
        );
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Folder {
                folder_id,
                properties: search_folder_handle_properties(
                    &definition,
                    folder_id,
                    principal.account_id,
                ),
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_create_folder_response(request, folder_id, false));
        output_handles.push(handle);
        return;
    }

    let existing_mailbox = mailboxes.iter().find(|mailbox| {
        mailbox.parent_id == create_parent_id && mailbox.name.eq_ignore_ascii_case(display_name)
    });
    let deleted_advertised_existing =
        create_folder_existing_mailbox_satisfies_deleted_advertised_request(
            session,
            parent_folder_id,
            display_name,
        );
    if request.create_folder_open_existing() || deleted_advertised_existing {
        if let Some(existing) = existing_mailbox {
            let folder_id = mapi_folder_id(existing);
            if deleted_advertised_existing && !request.create_folder_open_existing() {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x1c",
                    parent_folder_id = %format!("{parent_folder_id:#018x}"),
                    folder_id = %format!("{folder_id:#018x}"),
                    folder_type = request.create_folder_type(),
                    open_existing = request.create_folder_open_existing(),
                    display_name = display_name,
                    response_existing_folder = false,
                    message = "rca debug mapi create folder opened real folder replacing deleted advertised folder",
                );
            }
            let properties = folder_properties_for_open(
                store, principal, session, folder_id, mailboxes, snapshot,
            )
            .await;
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_create_folder_response(
                request,
                folder_id,
                if deleted_advertised_existing {
                    false
                } else {
                    private_create_folder_is_existing_response_flag()
                },
            ));
            output_handles.push(handle);
            return;
        }
    } else if existing_mailbox.is_some() {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x1c",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_type = request.create_folder_type(),
            open_existing = request.create_folder_open_existing(),
            display_name = display_name,
            response_error = "0x80040604",
            message = "rca debug mapi create folder duplicate name",
        );
        responses.extend_from_slice(&rop_error_response(
            0x1C,
            request.output_handle_index.unwrap_or(0),
            0x8004_0604,
        ));
        return;
    }

    match store
        .create_jmap_mailbox(
            JmapMailboxCreateInput {
                account_id: principal.account_id,
                name: display_name.to_string(),
                parent_id: create_parent_id,
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
                    return;
                }
            };
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x1c",
                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                folder_id = %format!("{folder_id:#018x}"),
                jmap_mailbox_id = %mailbox.id,
                folder_type = request.create_folder_type(),
                open_existing = request.create_folder_open_existing(),
                display_name = display_name,
                message = "rca debug mapi create folder created real folder",
            );
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties: HashMap::new(),
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_create_folder_response(request, folder_id, false));
            session.record_notification(MapiNotificationEvent::hierarchy(
                parent_folder_id,
                Some(folder_id),
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
