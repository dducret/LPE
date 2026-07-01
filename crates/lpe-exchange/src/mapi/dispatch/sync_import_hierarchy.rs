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
    if system_folder_display_name(&display_name) {
        if let Some(existing) =
            imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
        {
            record_sync_upload_hierarchy_change(session, folder_id, mapi_folder_id(existing));
        }
        responses.extend_from_slice(&rop_synchronization_import_hierarchy_change_response(
            request,
        ));
        return;
    }
    if let Some(existing) =
        imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
    {
        if existing.role == "custom" && existing.name.eq_ignore_ascii_case(&display_name) {
            record_sync_upload_hierarchy_change(session, folder_id, mapi_folder_id(existing));
            responses.extend_from_slice(&rop_synchronization_import_hierarchy_change_response(
                request,
            ));
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
            match remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::Mailbox,
                mailbox.id,
                None,
                None,
            )
            .await
            {
                Ok(_) => {}
                Err(_) => {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            record_sync_upload_hierarchy_change(session, folder_id, mapi_folder_id(&mailbox));
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
