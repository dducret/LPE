use super::*;

pub(super) async fn append_synchronization_import_hierarchy_change_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    responses: &mut Vec<u8>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x73,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
        Ok(values) => values,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x73,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
    };
    let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
    let Some(display_name) = display_name else {
        responses.extend_from_slice(&rop_error_response(
            0x73,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let source_key = hierarchy_values.iter().find_map(|(tag, value)| {
        (*tag == PID_TAG_SOURCE_KEY)
            .then_some(value)
            .and_then(|value| match value {
                MapiValue::Binary(bytes) => Some(bytes.clone()),
                _ => None,
            })
    });
    let Some((source_key, reserved_global_counter)) = source_key.and_then(|source_key| {
        persistable_import_source_key_global_counter(&source_key)
            .map(|counter| (source_key, counter))
    }) else {
        responses.extend_from_slice(&rop_error_response(
            0x73,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let alias_folder_id = crate::mapi::identity::mapi_store_id(reserved_global_counter);
    let parent_folder_id = hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(bytes)) if *tag == PID_TAG_PARENT_SOURCE_KEY => {
                crate::mapi::identity::object_id_from_source_key(bytes)
            }
            _ => None,
        })
        .map(|parent_id| session.resolve_special_folder_alias(parent_id))
        .unwrap_or_else(|| session.resolve_special_folder_alias(folder_id));
    if let Some(canonical_folder_id) =
        advertised_special_folder_id_for_create(parent_folder_id, &display_name)
    {
        if alias_folder_id != canonical_folder_id {
            let alias = MapiSpecialFolderAlias {
                alias_folder_id,
                canonical_folder_id,
                source_key,
            };
            let change_number = match store
                .upsert_mapi_special_folder_aliases(principal.account_id, &[alias])
                .await
                .ok()
                .and_then(|change_numbers| change_numbers.into_iter().next())
            {
                Some(change_number) => change_number,
                None => {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            session.record_special_folder_alias(alias_folder_id, canonical_folder_id);
            record_sync_upload_hierarchy_change_with_change_number(
                session,
                folder_id,
                canonical_folder_id,
                change_number,
            );
        }
        responses.extend_from_slice(&rop_synchronization_import_hierarchy_change_response(
            request,
        ));
        return;
    }
    if system_folder_display_name(&display_name) {
        responses.extend_from_slice(&rop_error_response(
            0x73,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    if let Some(existing) =
        imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
    {
        if existing.role == "custom" && existing.name.eq_ignore_ascii_case(&display_name) {
            match remember_created_mapi_identity_record(
                store,
                principal,
                MapiIdentityObjectKind::Mailbox,
                existing.id,
                Some(reserved_global_counter),
                Some(source_key),
            )
            .await
            {
                Ok(identity)
                    if identity.object_id == alias_folder_id
                        && identity.source_key
                            == crate::mapi::identity::source_key_for_object_id(alias_folder_id) =>
                {
                    record_sync_upload_hierarchy_change_with_change_number(
                        session,
                        folder_id,
                        identity.object_id,
                        identity.change_number,
                    );
                    responses.extend_from_slice(
                        &rop_synchronization_import_hierarchy_change_response(request),
                    );
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x73,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            }
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x73,
                request.response_handle_index(),
                0x8004_0102,
            ));
        }
        return;
    }

    let parent_id = imported_hierarchy_parent_mailbox_id(&hierarchy_values, folder_id, mailboxes);
    match store
        .create_jmap_mailbox(
            JmapMailboxCreateInput {
                account_id: principal.account_id,
                name: display_name.clone(),
                parent_id,
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
            let imported_identity = match remember_created_mapi_identity_record(
                store,
                principal,
                MapiIdentityObjectKind::Mailbox,
                mailbox.id,
                Some(reserved_global_counter),
                Some(source_key),
            )
            .await
            {
                Ok(identity) => identity,
                Err(_) => {
                    let _ = store
                        .destroy_jmap_mailbox(
                            principal.account_id,
                            mailbox.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-hierarchy-change-rollback".to_string(),
                                subject: display_name.clone(),
                            },
                        )
                        .await;
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            record_sync_upload_hierarchy_change_with_change_number(
                session,
                folder_id,
                imported_identity.object_id,
                imported_identity.change_number,
            );
            responses.extend_from_slice(&rop_synchronization_import_hierarchy_change_response(
                request,
            ));
        }
        Err(_) => responses.extend_from_slice(&rop_error_response(
            0x73,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}
