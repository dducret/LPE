use super::*;

pub(super) fn private_create_folder_is_existing_response_flag() -> bool {
    true
}

pub(super) fn is_receive_folder_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::SetReceiveFolder | RopId::GetReceiveFolder)
}

pub(super) fn append_receive_folder_dispatch_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SetReceiveFolder) => {
            append_set_receive_folder_response(
                principal,
                session,
                handle_slots,
                request,
                responses,
            );
            false
        }
        Some(RopId::GetReceiveFolder) => {
            append_get_receive_folder_response(
                principal,
                session,
                handle_slots,
                request,
                responses,
            );
            true
        }
        _ => false,
    }
}

pub(super) fn append_set_receive_folder_response(
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    if !private_logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            0x26,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(folder_id) = request.set_receive_folder_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x26,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    };
    let Some(message_class) = request.set_receive_folder_message_class() else {
        responses.extend_from_slice(&rop_error_response(
            0x26,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    };
    if !valid_receive_folder_message_class(message_class) {
        responses.extend_from_slice(&rop_error_response(
            0x26,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let canonical_folder_id = receive_folder_id_for_message_class(message_class);
    if folder_id != canonical_folder_id {
        responses.extend_from_slice(&rop_error_response(
            0x26,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        requested_message_class = %message_class,
        canonical_message_class =
            %explicit_receive_folder_message_class(message_class),
        canonical_folder_id = %format!("0x{canonical_folder_id:016x}"),
        "rca debug mapi canonical set receive folder accepted"
    );
    responses.extend_from_slice(&rop_simple_success_response(request));
}

pub(super) fn append_get_receive_folder_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    if !private_logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            0x27,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(message_class) = request.receive_folder_message_class() else {
        responses.extend_from_slice(&rop_error_response(
            0x27,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    };
    if !valid_receive_folder_message_class(message_class) {
        responses.extend_from_slice(&rop_error_response(
            0x27,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let response_folder_id = receive_folder_id_for_message_class(message_class);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x27",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        hierarchy_sync_completed = session.hierarchy_sync_completed(),
        requested_message_class = %message_class,
        response_message_class =
            %explicit_receive_folder_message_class(message_class),
        response_folder_id = %format!("0x{response_folder_id:016x}"),
        response_folder_is_calendar =
            response_folder_id == CALENDAR_FOLDER_ID,
        expected_calendar_folder_id = "0x0000000000100001",
        "rca debug mapi get receive folder resolution"
    );
    responses.extend_from_slice(&rop_get_receive_folder_response(
        request,
        response_folder_id,
        explicit_receive_folder_message_class(message_class),
    ));
    session.record_receive_folder_verification_passed();
    session.record_post_hierarchy_request_contract(post_hierarchy_get_receive_folder_contract(
        message_class,
        response_folder_id,
    ));
}

pub(super) async fn append_empty_folder_response<S: ExchangeStore>(
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
    if request.empty_folder_want_asynchronous().is_none()
        || request.empty_folder_want_delete_associated().is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x0000_04B9,
        ));
        return;
    };

    if folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let result = if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
        hard_delete_recoverable_folder_contents(store, principal, folder_id, snapshot).await
    } else if snapshot.public_folder_for_id(folder_id).is_some() {
        hard_delete_public_folder_contents(store, principal, folder_id, snapshot).await
    } else if request.rop_id == RopId::HardDeleteMessagesAndSubfolders.as_u8() {
        hard_delete_mailbox_tree_contents(store, principal, folder_id, mailboxes, emails, snapshot)
            .await
    } else {
        hard_delete_folder_contents(store, principal, folder_id, mailboxes, emails, snapshot).await
    };

    match result {
        Ok((changed_folder_ids, partial_completion)) => {
            for changed_folder_id in changed_folder_ids {
                session
                    .record_notification(MapiNotificationEvent::content(changed_folder_id, None));
            }
            responses.extend_from_slice(&rop_partial_completion_response(
                request.rop_id,
                request.response_handle_index(),
                partial_completion,
            ));
        }
        Err(error) => responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            error,
        )),
    }
}

pub(super) async fn append_delete_folder_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if request
        .delete_folder_flags()
        .is_none_or(|flags| flags & !0x15 != 0)
    {
        responses.extend_from_slice(&rop_error_response(
            0x1D,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let Some(parent_folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x1D,
            request.response_handle_index(),
            0x0000_04B9,
        ));
        return;
    };
    let Some(folder_id) = request.delete_folder_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x1D,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let mailbox = folder_row_for_id(folder_id, mailboxes);
    if let Some(mailbox) = mailbox {
        if mailbox.role != "custom" {
            responses.extend_from_slice(&rop_error_response(
                0x1D,
                request.response_handle_index(),
                0x8007_0005,
            ));
            return;
        }
    } else if is_advertised_special_folder(folder_id) {
        if !advertised_special_folder_delete_uses_session_tombstone(folder_id) {
            if advertised_special_folder_delete_is_noop(folder_id) {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = principal.email.as_str(),
                    request_type = "Execute",
                    request_rop_id = "0x1d",
                    parent_folder_id = %format!("{parent_folder_id:#018x}"),
                    folder_id = %format!("{folder_id:#018x}"),
                    partial_completion = false,
                    message = "rca debug mapi delete advertised special folder no-op acknowledged",
                );
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    false,
                ));
                return;
            }
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = principal.email.as_str(),
                request_type = "Execute",
                request_rop_id = "0x1d",
                parent_folder_id = %format!("{parent_folder_id:#018x}"),
                folder_id = %format!("{folder_id:#018x}"),
                response_error = "0x80070005",
                message = "rca debug mapi delete advertised special folder denied",
            );
            responses.extend_from_slice(&rop_error_response(
                0x1D,
                request.response_handle_index(),
                0x8007_0005,
            ));
            return;
        }
        session.record_deleted_advertised_special_folder(folder_id);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = principal.email.as_str(),
            request_type = "Execute",
            request_rop_id = "0x1d",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_id = %format!("{folder_id:#018x}"),
            partial_completion = false,
            message = "rca debug mapi delete advertised special folder acknowledged",
        );
        session.record_notification(MapiNotificationEvent::hierarchy(
            parent_folder_id,
            Some(folder_id),
        ));
        responses.extend_from_slice(&rop_partial_completion_response(
            0x1D,
            request.response_handle_index(),
            false,
        ));
        return;
    }
    if let Some(public_folder) = snapshot.public_folder_for_id(folder_id) {
        let partial_completion = store
            .delete_public_folder(
                principal.account_id,
                public_folder.folder.id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-delete-public-folder".to_string(),
                    subject: format!("public-folder:{}", public_folder.folder.id),
                },
            )
            .await
            .is_err();
        if !partial_completion {
            session.record_notification(MapiNotificationEvent::hierarchy(
                parent_folder_id,
                Some(folder_id),
            ));
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x1D,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    let persisted_search_definition = snapshot
        .search_folder_definition_for_folder_id(folder_id)
        .cloned();
    let staged_search_definition = if persisted_search_definition.is_none() {
        session.forget_search_folder_definition(folder_id)
    } else {
        None
    };
    if let Some(definition) = persisted_search_definition
        .as_ref()
        .or(staged_search_definition.as_ref())
    {
        if definition.is_builtin {
            responses.extend_from_slice(&rop_error_response(
                0x1D,
                request.response_handle_index(),
                0x8007_0005,
            ));
            return;
        }
        let partial_completion = if persisted_search_definition.is_some() {
            store
                .delete_search_folder(principal.account_id, definition.id)
                .await
                .is_err()
        } else {
            false
        };
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x1d",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_id = %format!("{folder_id:#018x}"),
            search_folder_id = %definition.id,
            display_name = %definition.display_name,
            partial_completion = partial_completion,
            message = "rca debug mapi delete search folder",
        );
        if !partial_completion {
            session.record_notification(MapiNotificationEvent::hierarchy(
                parent_folder_id,
                Some(folder_id),
            ));
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x1D,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    if session.search_folder_definition_was_deleted(folder_id) {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x1d",
            parent_folder_id = %format!("{parent_folder_id:#018x}"),
            folder_id = %format!("{folder_id:#018x}"),
            partial_completion = false,
            message = "rca debug mapi delete search folder retry acknowledged",
        );
        responses.extend_from_slice(&rop_partial_completion_response(
            0x1D,
            request.response_handle_index(),
            false,
        ));
        return;
    }
    let Some(mailbox) = mailbox else {
        responses.extend_from_slice(&rop_error_response(
            0x1D,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };

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
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x1d",
        parent_folder_id = %format!("{parent_folder_id:#018x}"),
        folder_id = %format!("{folder_id:#018x}"),
        jmap_mailbox_id = %mailbox.id,
        display_name = %mailbox.name,
        role = %mailbox.role,
        partial_completion = partial_completion,
        message = "rca debug mapi delete real folder",
    );
    if !partial_completion {
        session.record_notification(MapiNotificationEvent::hierarchy(
            parent_folder_id,
            Some(folder_id),
        ));
    }
    responses.extend_from_slice(&rop_partial_completion_response(
        0x1D,
        request.response_handle_index(),
        partial_completion,
    ));
}

pub(super) async fn append_folder_move_copy_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let rop_id = request.rop_id;
    let response_handle_index = request.response_handle_index();
    if request.folder_move_copy_want_asynchronous().is_none()
        || request.folder_move_copy_use_unicode().is_none()
        || (rop_id == RopId::CopyFolder.as_u8()
            && request.folder_move_copy_want_recursive().is_none())
    {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8007_0057,
        ));
        return;
    }
    let Some(source_parent_folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x0000_04B9,
        ));
        return;
    };
    let Some(target_folder_id) = request
        .move_copy_target_handle(handle_slots)
        .and_then(|handle| {
            session
                .handles
                .get(&handle)
                .and_then(|object| object.folder_id())
        })
    else {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8004_010F,
        ));
        return;
    };
    let Some(folder_id) = request.folder_move_copy_folder_id() else {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8004_0102,
        ));
        return;
    };
    let display_name = request.folder_move_copy_display_name();
    let display_name = display_name.trim();
    if display_name.is_empty() {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8004_0102,
        ));
        return;
    }

    if rop_id == RopId::CopyFolder.as_u8() {
        if let (Some(source_public_folder), Some(target_public_folder)) = (
            snapshot.public_folder_for_id(folder_id),
            snapshot.public_folder_for_id(target_folder_id),
        ) {
            let partial_completion = match copy_public_folder_tree_for_mapi(
                store,
                principal,
                source_public_folder.folder.id,
                target_public_folder.folder.id,
                display_name,
            )
            .await
            {
                Ok(copied_folder) => {
                    if let Ok(copied_folder_id) = remember_created_mapi_identity(
                        store,
                        principal,
                        MapiIdentityObjectKind::PublicFolder,
                        copied_folder.id,
                        None,
                        None,
                    )
                    .await
                    {
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            target_folder_id,
                            Some(copied_folder_id),
                        ));
                    }
                    false
                }
                Err(_) => true,
            };
            responses.extend_from_slice(&rop_partial_completion_response(
                rop_id,
                response_handle_index,
                partial_completion,
            ));
            return;
        }
    }
    if rop_id == RopId::MoveFolder.as_u8() {
        if let (Some(source_public_folder), Some(target_public_folder)) = (
            snapshot.public_folder_for_id(folder_id),
            snapshot.public_folder_for_id(target_folder_id),
        ) {
            let source_parent_matches = snapshot
                .public_folder_for_id(source_parent_folder_id)
                .map(|parent| {
                    source_public_folder.folder.parent_folder_id == Some(parent.folder.id)
                })
                .unwrap_or(false);
            if !source_parent_matches {
                responses.extend_from_slice(&rop_error_response(
                    rop_id,
                    response_handle_index,
                    0x8004_010F,
                ));
                return;
            }
            let partial_completion = store
                .update_public_folder(
                    UpdatePublicFolderInput {
                        account_id: principal.account_id,
                        folder_id: source_public_folder.folder.id,
                        parent_folder_id: Some(target_public_folder.folder.id),
                        display_name: Some(display_name.to_string()),
                        folder_class: None,
                        sort_order: None,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-move-public-folder".to_string(),
                        subject: format!(
                            "public-folder:{}->{}",
                            source_public_folder.folder.id, target_public_folder.folder.id
                        ),
                    },
                )
                .await
                .is_err();
            if !partial_completion {
                session.record_notification(MapiNotificationEvent::hierarchy(
                    source_parent_folder_id,
                    Some(folder_id),
                ));
                session.record_notification(MapiNotificationEvent::hierarchy(
                    target_folder_id,
                    Some(folder_id),
                ));
            }
            responses.extend_from_slice(&rop_partial_completion_response(
                rop_id,
                response_handle_index,
                partial_completion,
            ));
            return;
        }
    }

    let target_parent_id = match target_folder_id {
        IPM_SUBTREE_FOLDER_ID => None,
        folder_id => match folder_row_for_id(folder_id, mailboxes) {
            Some(mailbox) if mailbox.role == "custom" => Some(mailbox.id),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    rop_id,
                    response_handle_index,
                    0x8007_0005,
                ));
                return;
            }
        },
    };
    let Some(source_mailbox) = folder_row_for_id(folder_id, mailboxes) else {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8004_010F,
        ));
        return;
    };
    if source_mailbox.role != "custom" {
        responses.extend_from_slice(&rop_error_response(
            rop_id,
            response_handle_index,
            0x8007_0005,
        ));
        return;
    }

    let result = if request.rop_id == RopId::CopyFolder.as_u8() {
        match store
            .create_jmap_mailbox(
                JmapMailboxCreateInput {
                    account_id: principal.account_id,
                    name: display_name.to_string(),
                    parent_id: target_parent_id,
                    sort_order: None,
                    is_subscribed: source_mailbox.is_subscribed,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-copy-folder".to_string(),
                    subject: format!("folder:{}->{}", source_mailbox.id, display_name),
                },
            )
            .await
        {
            Ok(mailbox) => match remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::Mailbox,
                mailbox.id,
                None,
                None,
            )
            .await
            {
                Ok(copied_folder_id) => Ok((mailbox.id, copied_folder_id)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    } else {
        store
            .update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id: principal.account_id,
                    mailbox_id: source_mailbox.id,
                    name: Some(display_name.to_string()),
                    parent_id: Some(target_parent_id),
                    sort_order: None,
                    is_subscribed: None,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-move-folder".to_string(),
                    subject: format!("folder:{}", source_mailbox.id),
                },
            )
            .await
            .map(|mailbox| (mailbox.id, folder_id))
    };

    if let Ok((_changed_mailbox_id, changed_folder_id)) = result.as_ref() {
        let old_parent_folder_id = mailbox_parent_folder_id_for_dispatch(source_mailbox, mailboxes);
        let new_parent_folder_id = target_parent_id
            .and_then(|parent_id| {
                mailboxes
                    .iter()
                    .find(|mailbox| mailbox.id == parent_id)
                    .map(mapi_folder_id)
            })
            .unwrap_or(IPM_SUBTREE_FOLDER_ID);
        if request.rop_id == RopId::MoveFolder.as_u8() {
            session.record_notification(MapiNotificationEvent::hierarchy(
                old_parent_folder_id,
                Some(*changed_folder_id),
            ));
        }
        session.record_notification(MapiNotificationEvent::hierarchy(
            new_parent_folder_id,
            Some(*changed_folder_id),
        ));
    }
    let partial_completion = result.is_err();
    responses.extend_from_slice(&rop_partial_completion_response(
        rop_id,
        response_handle_index,
        partial_completion,
    ));
}

pub(super) async fn hard_delete_folder_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let mailbox = role_for_folder_id(folder_id)
        .and_then(|role| mailboxes.iter().find(|mailbox| mailbox.role == role))
        .or_else(|| {
            mailboxes.iter().find(|mailbox| {
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id) == Some(folder_id)
            })
        })
        .ok_or(0x8004_010Fu32)?;

    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_delete)
        .unwrap_or(true)
    {
        return Err(0x8007_0005);
    }

    let mut partial_completion = false;
    let mut changed_folder_ids = Vec::new();
    let message_ids = emails
        .iter()
        .filter(|email| email_matches_folder(email, folder_id, mailboxes))
        .map(|email| email.id)
        .collect::<Vec<_>>();
    let attempted_count = message_ids.len();
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;

    for message_id in message_ids {
        if store
            .delete_jmap_email_from_mailbox(
                principal.account_id,
                mailbox.id,
                message_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-hard-delete-folder-contents".to_string(),
                    subject: format!("folder:{} message:{}", mailbox.id, message_id),
                },
            )
            .await
            .is_err()
        {
            partial_completion = true;
            failed_count += 1;
        } else {
            if changed_folder_ids.is_empty() {
                changed_folder_ids.push(folder_id);
            }
            succeeded_count += 1;
        }
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete folder contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok((changed_folder_ids, partial_completion))
}

pub(super) async fn hard_delete_mailbox_tree_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let root_mailbox = role_for_folder_id(folder_id)
        .and_then(|role| mailboxes.iter().find(|mailbox| mailbox.role == role))
        .or_else(|| {
            mailboxes.iter().find(|mailbox| {
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id) == Some(folder_id)
            })
        })
        .ok_or(0x8004_010Fu32)?;

    let mut target_mailboxes = Vec::new();
    for mailbox in mailboxes {
        let mut current = Some(mailbox.id);
        let mut visited = HashSet::new();
        while let Some(current_id) = current {
            if current_id == root_mailbox.id {
                target_mailboxes.push(mailbox);
                break;
            }
            if !visited.insert(current_id) {
                break;
            }
            current = mailboxes
                .iter()
                .find(|candidate| candidate.id == current_id)
                .and_then(|candidate| candidate.parent_id);
        }
    }

    let target_folder_ids = target_mailboxes
        .iter()
        .map(|mailbox| {
            (
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
                    .unwrap_or_else(|| mapi_folder_id(mailbox)),
                mailbox.id,
            )
        })
        .collect::<Vec<_>>();

    for (target_folder_id, _) in &target_folder_ids {
        if !snapshot
            .folder_access_for_principal(*target_folder_id, principal.account_id)
            .map(|access| access.may_delete)
            .unwrap_or(true)
        {
            return Err(0x8007_0005);
        }
    }

    let mut partial_completion = false;
    let mut changed_folder_ids = Vec::new();
    let mut attempted_count = 0usize;
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;
    for (target_folder_id, mailbox_id) in target_folder_ids {
        let message_ids = emails
            .iter()
            .filter(|email| email_matches_folder(email, target_folder_id, mailboxes))
            .map(|email| email.id)
            .collect::<Vec<_>>();
        attempted_count += message_ids.len();
        for message_id in message_ids {
            if store
                .delete_jmap_email_from_mailbox(
                    principal.account_id,
                    mailbox_id,
                    message_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-hard-delete-folder-tree-contents".to_string(),
                        subject: format!("folder:{mailbox_id} message:{message_id}"),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
                failed_count += 1;
            } else {
                if !changed_folder_ids.contains(&target_folder_id) {
                    changed_folder_ids.push(target_folder_id);
                }
                succeeded_count += 1;
            }
        }
    }
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete folder tree contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok((changed_folder_ids, partial_completion))
}

pub(super) fn collaboration_folder_handle_properties(
    folder: &crate::mapi_store::MapiCollaborationFolder,
) -> HashMap<u32, MapiValue> {
    [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_DELETED_COUNT_TOTAL,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_FOLDER_FORM_STORAGE,
        PID_TAG_ACL_MEMBER_NAME_W,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIER_REV,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
    .into_iter()
    .filter_map(|tag| collaboration_folder_property_value(folder, tag).map(|value| (tag, value)))
    .collect()
}

pub(super) fn create_folder_existing_mailbox_satisfies_deleted_advertised_request(
    session: &MapiSession,
    parent_folder_id: u64,
    display_name: &str,
) -> bool {
    advertised_special_folder_id_for_create(parent_folder_id, display_name)
        .map(|folder_id| session.advertised_special_folder_was_deleted(folder_id))
        .unwrap_or(false)
}

pub(super) fn advertised_special_folder_delete_uses_session_tombstone(folder_id: u64) -> bool {
    folder_id == QUICK_STEP_SETTINGS_FOLDER_ID
}

pub(super) fn advertised_special_folder_delete_is_noop(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
    )
}

pub(super) fn synthetic_folder_allows_create_message(folder_id: u64) -> bool {
    matches!(
        folder_id,
        INBOX_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
    )
}

pub(super) fn advertised_special_folder_container_class(folder_id: u64) -> Option<&'static str> {
    role_for_folder_id(folder_id)?;
    Some(match folder_id {
        CALENDAR_FOLDER_ID => "IPF.Appointment",
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID => {
            "IPF.Contact"
        }
        QUICK_CONTACTS_FOLDER_ID => "IPF.Contact.MOC.QuickContacts",
        IM_CONTACT_LIST_FOLDER_ID => "IPF.Contact.MOC.ImContactList",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPF.Task",
        NOTES_FOLDER_ID => "IPF.StickyNote",
        JOURNAL_FOLDER_ID => "IPF.Journal",
        RSS_FEEDS_FOLDER_ID => "IPF.Note.OutlookHomepage",
        _ => "IPF.Note",
    })
}

pub(super) async fn folder_properties_for_open<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> HashMap<u32, MapiValue>
where
    S: ExchangeStore,
{
    let mut properties =
        folder_properties_for_open_from_mailboxes(principal, folder_id, mailboxes, snapshot);
    if !session.search_folder_definition_was_deleted(folder_id) {
        if let Some(definition) = session.search_folder_definition(folder_id) {
            properties.extend(search_folder_handle_properties(
                definition,
                folder_id,
                principal.account_id,
            ));
        }
    }
    if folder_id == IPM_SUBTREE_FOLDER_ID {
        if let Ok(Some(ost_id)) = store
            .fetch_mapi_ipm_subtree_ost_id(principal.account_id)
            .await
        {
            properties.insert(PID_TAG_OST_OSTID, MapiValue::Binary(ost_id));
        }
    }
    if let Ok(values) = store
        .fetch_mapi_folder_profile_property_values(
            principal.account_id,
            folder_id,
            &[PID_TAG_EXTENDED_FOLDER_FLAGS],
        )
        .await
    {
        for value in values {
            if value.property_tag == PID_TAG_EXTENDED_FOLDER_FLAGS {
                properties.insert(
                    PID_TAG_EXTENDED_FOLDER_FLAGS,
                    MapiValue::Binary(value.property_value),
                );
            }
        }
    }
    properties
}

pub(super) async fn persist_profile_folder_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Result<()>
where
    S: ExchangeStore,
{
    let folder_profile_values = values
        .iter()
        .filter_map(|(tag, value)| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if storage_tag != PID_TAG_EXTENDED_FOLDER_FLAGS {
                return None;
            }
            let MapiValue::Binary(bytes) = value else {
                return None;
            };
            Some(crate::store::MapiFolderProfilePropertyValue {
                folder_id,
                property_tag: storage_tag,
                property_type: (PID_TAG_EXTENDED_FOLDER_FLAGS & 0xffff) as u16,
                property_value: bytes.clone(),
            })
        })
        .collect::<Vec<_>>();
    if !folder_profile_values.is_empty() {
        store
            .upsert_mapi_folder_profile_property_values(
                principal.account_id,
                &folder_profile_values,
            )
            .await?;
    }
    if folder_id != IPM_SUBTREE_FOLDER_ID {
        return Ok(());
    }
    for (tag, value) in values {
        if canonical_property_storage_tag(*tag) == PID_TAG_OST_OSTID {
            if let MapiValue::Binary(ost_id) = value {
                store
                    .store_mapi_ipm_subtree_ost_id(principal.account_id, ost_id)
                    .await?;
            }
        }
    }
    Ok(())
}

pub(super) fn folder_properties_for_open_from_mailboxes(
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> HashMap<u32, MapiValue> {
    let mut properties = HashMap::new();
    let open_folder_property_tags = [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_ASSOCIATED_CONTENT_COUNT,
        PID_TAG_DELETED_COUNT_TOTAL,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_RIGHTS,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_FOLDER_FORM_STORAGE,
        PID_TAG_ACL_MEMBER_NAME_W,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIER_REV,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ];
    let mailbox = folder_row_for_id(folder_id, mailboxes);
    if let Some(mailbox) = mailbox {
        for property_tag in open_folder_property_tags {
            if let Some(value) = mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                property_tag,
                principal.account_id,
            ) {
                properties.insert(property_tag, value);
            }
        }
    }
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        properties.extend(collaboration_folder_handle_properties(folder));
    }
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        for property_tag in open_folder_property_tags {
            if let Some(value) = public_folder_property_value(folder, property_tag) {
                properties.insert(property_tag, value);
            }
        }
    }
    if let Some(definition) = snapshot.search_folder_definition_for_folder_id(folder_id) {
        properties.extend(search_folder_handle_properties(
            definition,
            folder_id,
            principal.account_id,
        ));
    }
    if is_advertised_special_folder(folder_id) {
        for property_tag in open_folder_property_tags {
            if property_tag == PID_TAG_PARENT_SOURCE_KEY
                && matches!(folder_id, ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID)
            {
                continue;
            }
            if !properties.contains_key(&property_tag) {
                if let Some(value) =
                    special_folder_property_value(folder_id, property_tag, principal.account_id)
                {
                    properties.insert(property_tag, value);
                }
            }
        }
    }
    if mailbox.is_none() && is_advertised_special_folder(folder_id) {
        let (content_count, unread_count) = snapshot_message_counts_for_folder(snapshot, folder_id);
        properties.insert(PID_TAG_CONTENT_COUNT, MapiValue::U32(content_count));
        properties.insert(PID_TAG_CONTENT_UNREAD_COUNT, MapiValue::U32(unread_count));
    }
    if folder_id == INBOX_FOLDER_ID {
        if let Some(value) =
            special_folder_property_value(folder_id, PID_TAG_DISPLAY_NAME_W, principal.account_id)
        {
            properties.insert(PID_TAG_DISPLAY_NAME_W, value);
        }
    }
    properties.insert(
        PID_TAG_ASSOCIATED_CONTENT_COUNT,
        MapiValue::U32(associated_folder_message_count(folder_id, snapshot)),
    );
    properties
}

pub(super) fn folder_local_default_named_view_is_supported(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> bool {
    snapshot
        .default_folder_named_view_message(folder_id, message_id)
        .is_some_and(|_| {
            let container_class = snapshot
                .collaboration_folder_for_id(folder_id)
                .map(|folder| collaboration_folder_message_class(folder.kind))
                .or_else(|| advertised_special_folder_container_class(folder_id));
            container_class.is_some_and(|container_class| {
                default_view_supported_folder(folder_id, container_class)
                    && !default_view_uses_common_views(container_class, folder_id)
            })
        })
}

pub(super) fn snapshot_message_counts_for_folder(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
) -> (u32, u32) {
    let emails = snapshot.emails();
    let count = folder_message_count(folder_id, &[], &emails, snapshot);
    let unread = emails
        .iter()
        .filter(|email| snapshot_email_belongs_to_folder(email, folder_id) && email.unread)
        .count();
    (count, unread.min(u32::MAX as usize) as u32)
}

fn snapshot_email_belongs_to_folder(email: &JmapEmail, folder_id: u64) -> bool {
    email_role_folder_id(&email.mailbox_role) == Some(folder_id)
        || email
            .mailbox_states
            .iter()
            .any(|state| email_role_folder_id(&state.role) == Some(folder_id))
}

fn email_role_folder_id(role: &str) -> Option<u64> {
    crate::mapi_store::reserved_folder_counter_for_role(role)
        .map(crate::mapi::identity::mapi_store_id)
}

pub(super) fn mailbox_parent_folder_id_for_dispatch(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if mailbox.role == "__mapi_collaboration_calendar" {
        return IPM_SUBTREE_FOLDER_ID;
    }
    mailbox
        .parent_id
        .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
        .map(mapi_folder_id)
        .unwrap_or(IPM_SUBTREE_FOLDER_ID)
}

pub(super) fn mailbox_is_trash_or_descendant(mailbox_id: Uuid, mailboxes: &[JmapMailbox]) -> bool {
    let mut current = Some(mailbox_id);
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return false;
        }
        let Some(mailbox) = mailboxes.iter().find(|candidate| candidate.id == id) else {
            return false;
        };
        if mailbox.role == "trash" {
            return true;
        }
        current = mailbox.parent_id;
    }
    false
}
