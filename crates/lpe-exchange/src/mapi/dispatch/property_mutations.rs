use super::*;

pub(super) enum PropertyMutationFlow {
    Continue,
    StopBatch,
}

pub(super) async fn append_set_properties_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> PropertyMutationFlow
where
    S: ExchangeStore,
{
    let set_properties_object = input_object(session, handle_slots, request).cloned();
    let set_properties_probe = set_properties_probe_request(request);
    log_set_properties_specific_debug(
        principal,
        request_id,
        request,
        set_properties_object.as_ref(),
        &set_properties_probe,
    );
    if let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = set_properties_object.as_ref()
    {
        let existing = associated_config_message_for_mutation(
            snapshot,
            *folder_id,
            *config_id,
            saved_message.as_ref(),
        );
        let existing_property_tags = existing
            .as_ref()
            .map(|message| {
                let mut tags = mapi_properties_from_json(&message.properties_json)
                    .into_keys()
                    .collect::<Vec<_>>();
                tags.sort_unstable();
                format_debug_property_tags(&tags)
            })
            .unwrap_or_default();
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = %rop_id_hex(request.rop_id),
            input_handle_index = request.input_handle_index().unwrap_or(0),
            folder_id = format_args!("0x{folder_id:016x}"),
            config_id = format_args!("0x{config_id:016x}"),
            existing_property_tags = %existing_property_tags,
            property_tags = %format_debug_property_tags(&set_properties_probe.property_tags),
            property_value_shapes = %set_properties_probe.property_value_shapes,
            associated_config_stream_summary = %set_properties_probe.associated_config_stream_summary,
            parse_error = %set_properties_probe.parse_error,
            "rca debug mapi set associated config properties"
        );
    }
    session.record_recent_probe_action(format!(
        "{}(in={},kind={},folder={},tags={})",
        rop_id_hex(request.rop_id),
        request.input_handle_index().unwrap_or(0),
        mapi_object_debug_kind(set_properties_object.as_ref()),
        mapi_object_debug_folder_id(set_properties_object.as_ref()),
        format_debug_property_tags(&set_properties_probe.property_tags)
    ));
    let requested_values = match request.property_values() {
        Ok(values) => values,
        Err(_) => {
            let response =
                rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102);
            let post_hierarchy_contract = post_hierarchy_setprops_contract(
                request,
                set_properties_object.as_ref(),
                &set_properties_probe,
                &response,
            );
            session.record_post_hierarchy_setprops_contract(post_hierarchy_contract.clone());
            session.record_post_hierarchy_request_contract(format!(
                "{post_hierarchy_contract}->error"
            ));
            responses.extend_from_slice(&response);
            return PropertyMutationFlow::StopBatch;
        }
    };
    let requested_property_tags = requested_values
        .iter()
        .map(|(tag, _)| *tag)
        .collect::<Vec<_>>();
    let values = requested_values
        .into_iter()
        .map(|(tag, value)| (session.normalize_named_property_tag(tag), value))
        .collect::<Vec<_>>();
    let mut event_property_problems = Vec::new();
    let set_result = if let Some(result) = stage_virtual_conversation_action_property_values(
        session,
        handle_slots,
        request,
        snapshot,
        values.clone(),
    ) {
        result
    } else {
        match set_properties_object.clone() {
            Some(MapiObject::Message { .. }) => {
                stage_message_property_values(session, handle_slots, request, values)
            }
            Some(MapiObject::Event { .. }) => {
                stage_event_property_values(session, handle_slots, request, snapshot, values)
                    .map(|problems| event_property_problems = problems)
            }
            Some(MapiObject::PendingEvent { .. }) => stage_pending_event_property_values(
                session,
                handle_slots,
                request,
                principal,
                values,
            )
            .map(|problems| event_property_problems = problems),
            Some(MapiObject::NavigationShortcut { .. }) => {
                stage_existing_navigation_shortcut_property_values(
                    principal,
                    session,
                    handle_slots,
                    request,
                    snapshot,
                    values,
                )
            }
            Some(MapiObject::AssociatedConfig {
                folder_id,
                config_id,
                saved_message,
            }) => {
                match associated_config_message_for_mutation(
                    snapshot,
                    folder_id,
                    config_id,
                    saved_message.as_ref(),
                ) {
                    Some(existing) => match set_associated_config_properties(&existing, values) {
                        Ok(saved) => {
                            if let Some(MapiObject::AssociatedConfig { saved_message, .. }) =
                                input_object_mut(session, handle_slots, request)
                            {
                                *saved_message = Some(saved);
                            }
                            Ok(())
                        }
                        Err(error) => Err(error),
                    },
                    None => Err(anyhow!("MAPI associated config message was not found")),
                }
            }
            Some(
                object @ (MapiObject::Contact { .. }
                | MapiObject::Task { .. }
                | MapiObject::Note { .. }
                | MapiObject::JournalEntry { .. }
                | MapiObject::ConversationAction { .. }
                | MapiObject::DelegateFreeBusyMessage { .. }
                | MapiObject::PublicFolderItem { .. }
                | MapiObject::Attachment { .. }),
            ) => {
                apply_supported_object_property_values(
                    store, principal, &object, values, mailboxes, emails, snapshot,
                )
                .await
            }
            object @ Some(MapiObject::Folder { .. }) => {
                let problems = folder_set_property_problems(object.as_ref(), mailboxes, &values);
                if !problems.is_empty() {
                    let response = rop_set_properties_problem_response(request, &problems);
                    log_set_properties_default_folder_response_debug(
                        principal,
                        request_id,
                        request,
                        object.as_ref(),
                        &set_properties_probe,
                        &response,
                    );
                    let post_hierarchy_contract = post_hierarchy_setprops_contract(
                        request,
                        object.as_ref(),
                        &set_properties_probe,
                        &response,
                    );
                    session
                        .record_post_hierarchy_setprops_contract(post_hierarchy_contract.clone());
                    session.record_post_hierarchy_request_contract(format!(
                        "{post_hierarchy_contract}->problems"
                    ));
                    responses.extend_from_slice(&response);
                    return PropertyMutationFlow::Continue;
                }
                let aliases = default_folder_entry_id_aliases(object.as_ref(), &values);
                match store
                    .upsert_mapi_special_folder_aliases(principal.account_id, &aliases)
                    .await
                {
                    Err(error) => Err(error),
                    Ok(_change_numbers) => {
                        for alias in aliases {
                            session.record_special_folder_alias(
                                alias.alias_folder_id,
                                alias.canonical_folder_id,
                            );
                        }
                        let values = default_folder_identification_safe_property_values(
                            principal,
                            object.as_ref(),
                            values,
                        );
                        let result = apply_mapi_property_values(
                            input_object_mut(session, handle_slots, request),
                            values.clone(),
                        );
                        if result.is_ok() {
                            if let Some(MapiObject::Folder { folder_id, .. }) = object {
                                if persist_profile_folder_property_values(
                                    store, principal, folder_id, &values,
                                )
                                .await
                                .is_err()
                                {
                                    tracing::warn!(
                                        adapter = "mapi",
                                        endpoint = "emsmdb",
                                        mailbox = %principal.email,
                                        folder_id = format_args!("0x{folder_id:016x}"),
                                        property_tags = %format_debug_property_tags(
                                            &values.iter().map(|(tag, _value)| *tag).collect::<Vec<_>>()
                                        ),
                                        "accepted MAPI folder property write but failed to persist profile state"
                                    );
                                }
                            }
                        }
                        result
                    }
                }
            }
            _object => {
                apply_mapi_property_values(input_object_mut(session, handle_slots, request), values)
            }
        }
    };
    match set_result {
        Ok(()) => {
            restore_requested_property_problem_tags(
                &requested_property_tags,
                &mut event_property_problems,
            );
            // [MS-OXCPRPT] sections 3.2.5.4 and 3.2.5.5: valid properties in
            // a mixed request succeed while invalid properties are reported.
            let response = if event_property_problems.is_empty() {
                rop_set_properties_response(request)
            } else {
                rop_set_properties_problem_response(request, &event_property_problems)
            };
            log_set_properties_default_folder_response_debug(
                principal,
                request_id,
                request,
                set_properties_object.as_ref(),
                &set_properties_probe,
                &response,
            );
            let post_hierarchy_contract = post_hierarchy_setprops_contract(
                request,
                set_properties_object.as_ref(),
                &set_properties_probe,
                &response,
            );
            session.record_post_hierarchy_setprops_contract(post_hierarchy_contract.clone());
            session
                .record_post_hierarchy_request_contract(format!("{post_hierarchy_contract}->ok"));
            responses.extend_from_slice(&response);
        }
        Err(_) => {
            let response =
                rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102);
            log_set_properties_default_folder_response_debug(
                principal,
                request_id,
                request,
                set_properties_object.as_ref(),
                &set_properties_probe,
                &response,
            );
            let post_hierarchy_contract = post_hierarchy_setprops_contract(
                request,
                set_properties_object.as_ref(),
                &set_properties_probe,
                &response,
            );
            session.record_post_hierarchy_setprops_contract(post_hierarchy_contract.clone());
            session.record_post_hierarchy_request_contract(format!(
                "{post_hierarchy_contract}->error"
            ));
            responses.extend_from_slice(&response);
        }
    }
    PropertyMutationFlow::Continue
}

pub(super) async fn append_delete_properties_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let requested_property_tags = request.property_tags();
    let property_tags = requested_property_tags
        .iter()
        .copied()
        .into_iter()
        .map(|tag| session.normalize_named_property_tag(tag))
        .collect::<Vec<_>>();
    let object = input_object(session, handle_slots, request).cloned();
    let mut event_property_problems = Vec::new();
    let delete_result = if let Some(result) = stage_virtual_conversation_action_property_delete(
        session,
        handle_slots,
        request,
        snapshot,
        &property_tags,
    ) {
        result
    } else if matches!(object, Some(MapiObject::Event { .. })) {
        stage_event_property_deletions(session, handle_slots, request, snapshot, &property_tags)
            .map(|problems| event_property_problems = problems)
    } else if matches!(object, Some(MapiObject::NavigationShortcut { .. })) {
        stage_existing_navigation_shortcut_property_deletions(
            principal,
            session,
            handle_slots,
            request,
            snapshot,
            &property_tags,
        )
        .map(|problems| event_property_problems = problems)
    } else if let Some(MapiObject::ConversationAction {
        folder_id,
        conversation_action_id,
        ..
    }) = object
    {
        delete_conversation_action_properties(
            store,
            principal,
            folder_id,
            conversation_action_id,
            snapshot,
            &property_tags,
            mailboxes,
            emails,
        )
        .await
    } else if let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = object
    {
        let result = delete_associated_config_properties(
            folder_id,
            config_id,
            snapshot,
            saved_message.as_ref(),
            &property_tags,
        );
        if let Ok((deleted_property_count, saved)) = &result {
            if let Some(MapiObject::AssociatedConfig { saved_message, .. }) =
                input_object_mut(session, handle_slots, request)
            {
                *saved_message = Some(saved.clone());
            }
            tracing::info!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = format_args!("0x{:02x}", request.rop_id),
                folder_id = format_args!("0x{folder_id:016x}"),
                config_id = format_args!("0x{config_id:016x}"),
                property_tags = %format_debug_property_tags(&property_tags),
                deleted_property_count,
                "rca debug mapi delete associated config properties"
            );
        }
        result.map(|_| ())
    } else {
        let custom_delete_result = delete_custom_property_values(
            store,
            principal,
            object.as_ref(),
            mailboxes,
            emails,
            snapshot,
            &property_tags,
        )
        .await;
        match custom_delete_result {
            Ok(()) => {
                let canonical_delete_result = delete_canonical_message_text_properties(
                    store,
                    principal,
                    object.as_ref(),
                    &property_tags,
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await;
                canonical_delete_result.and_then(|_| {
                    delete_mapi_properties(
                        input_object_mut(session, handle_slots, request),
                        &property_tags,
                    )
                    .or_else(|error| {
                        if property_tags.iter().all(|tag| is_custom_property_tag(*tag)) {
                            Ok(())
                        } else if persisted_object_property_delete_is_idempotent(
                            object.as_ref(),
                            &property_tags,
                            snapshot,
                        ) {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                request_rop_id = %format!("{:#04x}", request.rop_id),
                                object_kind = mapi_object_debug_kind(object.as_ref()),
                                folder_id = %mapi_object_debug_folder_id(object.as_ref()),
                                property_tags = %format_debug_property_tags(&property_tags),
                                delete_error = %error,
                                fallback_reason = "persisted_message_best_effort_delete",
                                "rca debug mapi delete properties fallback"
                            );
                            Ok(())
                        } else {
                            Err(error)
                        }
                    })
                })
            }
            Err(error) => Err(error),
        }
    };
    match delete_result {
        Ok(()) => {
            restore_requested_property_problem_tags(
                &requested_property_tags,
                &mut event_property_problems,
            );
            let response = if event_property_problems.is_empty() {
                rop_delete_properties_response(request)
            } else {
                rop_set_properties_problem_response(request, &event_property_problems)
            };
            responses.extend_from_slice(&response);
        }
        Err(_) => responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

fn restore_requested_property_problem_tags(
    requested_tags: &[u32],
    problems: &mut [(usize, u32, u32)],
) {
    for (index, tag, _) in problems {
        if let Some(requested_tag) = requested_tags.get(*index) {
            *tag = *requested_tag;
        }
    }
}

fn persisted_object_property_delete_is_idempotent(
    object: Option<&MapiObject>,
    property_tags: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    let Some(MapiObject::Event {
        folder_id,
        event_id,
        ..
    }) = object
    else {
        return persisted_message_delete_is_best_effort(object);
    };
    let Some(event) = snapshot.event_for_id(*folder_id, *event_id) else {
        return false;
    };
    let reminder = snapshot.reminder_for_source("calendar", event.canonical_id);
    property_tags.iter().all(|property_tag| {
        event_property_value_with_reminder(
            &event.event,
            event.id,
            event.folder_id,
            *property_tag,
            reminder,
        )
        .is_none()
    })
}
