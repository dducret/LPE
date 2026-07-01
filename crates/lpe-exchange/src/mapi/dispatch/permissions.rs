use super::*;

pub(super) fn is_permissions_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetPermissionsTable | RopId::ModifyPermissions
    )
}

pub(super) fn append_get_permissions_table_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_handle_index_error_response(request));
        return;
    };
    if folder_row_for_id(folder_id, mailboxes).is_none()
        && role_for_folder_id(folder_id).is_none()
        && !is_advertised_special_folder(folder_id)
        && snapshot.public_folder_for_id(folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x3E,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    }
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        permission_table_object(folder_id),
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&get_permissions_table_response(request));
    output_handles.push(handle);
}

pub(super) async fn append_modify_permissions_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_handle_index_error_response(request));
        return;
    };
    let mailbox_folder = folder_row_for_id(folder_id, mailboxes);
    let public_folder = snapshot.public_folder_for_id(folder_id);
    let calendar_collection_folder = snapshot
        .collaboration_folder_for_id(folder_id)
        .filter(|folder| folder.kind == MapiCollaborationFolderKind::Calendar);
    let default_calendar_folder = role_for_folder_id(folder_id) == Some("calendar");
    if mailbox_folder.is_none()
        && public_folder.is_none()
        && calendar_collection_folder.is_none()
        && !default_calendar_folder
    {
        responses.extend_from_slice(&rop_error_response(
            0x40,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    let can_share = if default_calendar_folder {
        true
    } else if let Some(folder) = calendar_collection_folder {
        folder.collection.rights.may_share
    } else if let Some(public_folder) = public_folder {
        public_folder.folder.rights.may_share
    } else {
        snapshot
            .permissions_for_folder(folder_id)
            .iter()
            .find(|permission| permission.member_account_id == Some(principal.account_id))
            .is_some_and(|permission| may_share_from_rights(permission.rights))
    };
    if !can_share {
        responses.extend_from_slice(&rop_error_response(
            0x40,
            request.response_handle_index(),
            EC_SEARCH_ACCESS_DENIED,
        ));
        return;
    }

    let rows = match request.modify_permissions_rows() {
        Ok(rows) => rows,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x40,
                request.response_handle_index(),
                EC_RULE_INVALID_PARAMETER,
            ));
            return;
        }
    };
    let mut actions = Vec::new();
    let mut failed = None;
    for row in rows {
        let row_kind = row.flags & (ROW_ADD | ROW_MODIFY | ROW_REMOVE);
        if !matches!(row_kind, ROW_ADD | ROW_MODIFY | ROW_REMOVE) {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        }
        let Some(member_id) = row
            .properties
            .get(&PID_TAG_MEMBER_ID)
            .and_then(MapiValue::as_i64)
            .and_then(|value| u64::try_from(value).ok())
        else {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        };
        if member_id == MEMBER_ID_DEFAULT || member_id == MEMBER_ID_ANONYMOUS {
            continue;
        }
        let member_ids = [member_id];
        let identity = match store
            .fetch_mapi_identities_by_object_ids(principal.account_id, &member_ids)
            .await
        {
            Ok(mut identities) => identities.pop(),
            Err(_) => None,
        };
        let Some(identity) =
            identity.filter(|identity| identity.object_kind == MapiIdentityObjectKind::Account)
        else {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        };
        if identity.canonical_id == principal.account_id {
            continue;
        }
        let (may_read, may_write, may_delete, may_share) = if row_kind == ROW_REMOVE {
            (false, false, false, false)
        } else {
            let Some(rights) = row
                .properties
                .get(&PID_TAG_MEMBER_RIGHTS)
                .and_then(MapiValue::as_i64)
                .and_then(|value| u32::try_from(value).ok())
            else {
                failed = Some(EC_RULE_INVALID_PARAMETER);
                break;
            };
            let access = access_from_rights(rights);
            (
                access.may_read,
                access.may_write,
                access.may_delete,
                may_share_from_rights(rights),
            )
        };
        if !may_read && (may_write || may_delete || may_share) {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        }
        if may_delete && !may_write {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        }
        if may_share && !may_write {
            failed = Some(EC_RULE_INVALID_PARAMETER);
            break;
        }
        actions.push((
            row_kind,
            identity.canonical_id,
            may_read,
            may_write,
            may_delete,
            may_share,
        ));
    }
    if let Some(error_code) = failed {
        responses.extend_from_slice(&rop_error_response(
            0x40,
            request.response_handle_index(),
            error_code,
        ));
        return;
    }
    let mut failed = false;
    if default_calendar_folder {
        for (_row_kind, grantee_account_id, may_read, may_write, may_delete, may_share) in actions {
            if store
                .set_mapi_calendar_permission(
                    principal.account_id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-modify-calendar-permissions".to_string(),
                        subject: format!("calendar {grantee_account_id}"),
                    },
                )
                .await
                .is_err()
            {
                failed = true;
                break;
            }
        }
    } else if let Some(folder) = calendar_collection_folder {
        for (_row_kind, grantee_account_id, may_read, may_write, may_delete, may_share) in actions {
            if store
                .set_mapi_calendar_collection_permission(
                    folder.collection.owner_account_id,
                    &folder.collection.id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-modify-calendar-permissions".to_string(),
                        subject: format!(
                            "calendar {} {}",
                            folder.collection.id, grantee_account_id
                        ),
                    },
                )
                .await
                .is_err()
            {
                failed = true;
                break;
            }
        }
    } else if let Some(folder) = mailbox_folder {
        for (_row_kind, grantee_account_id, may_read, may_write, may_delete, may_share) in actions {
            if store
                .set_mapi_folder_permission(
                    principal.account_id,
                    folder.id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-modify-permissions".to_string(),
                        subject: format!("folder {} {}", folder.name, grantee_account_id),
                    },
                )
                .await
                .is_err()
            {
                failed = true;
                break;
            }
        }
    } else if let Some(folder) = public_folder {
        for (row_kind, grantee_account_id, may_read, may_write, may_delete, may_share) in actions {
            let audit = AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-modify-public-folder-permissions".to_string(),
                subject: format!(
                    "public folder {} {}",
                    folder.folder.display_name, grantee_account_id
                ),
            };
            let result = if row_kind == ROW_REMOVE {
                store
                    .delete_public_folder_permission(
                        principal.account_id,
                        folder.folder.id,
                        grantee_account_id,
                        audit,
                    )
                    .await
                    .map(|_| ())
            } else {
                store
                    .upsert_public_folder_permission(
                        PublicFolderPermissionInput {
                            account_id: principal.account_id,
                            public_folder_id: folder.folder.id,
                            principal_account_id: grantee_account_id,
                            may_read,
                            may_write,
                            may_delete,
                            may_share,
                        },
                        audit,
                    )
                    .await
                    .map(|_| ())
            };
            if result.is_err() {
                failed = true;
                break;
            }
        }
    }
    if failed {
        responses.extend_from_slice(&rop_error_response(
            0x40,
            request.response_handle_index(),
            EC_RULE_INVALID_PARAMETER,
        ));
        return;
    }
    responses.extend_from_slice(&rop_modify_permissions_response(request))
}

pub(super) async fn append_permissions_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetPermissionsTable) => {
            append_get_permissions_table_response(
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::ModifyPermissions) => {
            append_modify_permissions_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
            )
            .await;
        }
        _ => {}
    }
}
