use super::*;

pub(super) enum PropertyMutationFlow {
    Continue,
    StopBatch,
}

fn clear_folder_profile_property_tombstones(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    values: &[(u32, MapiValue)],
) {
    let Some(handle) = input_handle(handle_slots, request) else {
        return;
    };
    let Some(tombstones) = session.folder_profile_property_tombstones.get_mut(&handle) else {
        return;
    };
    for (property_tag, _) in values {
        let property_tag = canonical_property_storage_tag(*property_tag);
        if is_lazy_folder_profile_property_tag(property_tag) {
            tombstones.remove(&property_tag);
        }
    }
    if tombstones.is_empty() {
        session.folder_profile_property_tombstones.remove(&handle);
    }
}

fn mark_folder_profile_property_tombstones(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    object: Option<&MapiObject>,
    property_tags: &[u32],
) {
    if !matches!(object, Some(MapiObject::Folder { .. })) {
        return;
    }
    let Some(handle) = input_handle(handle_slots, request) else {
        return;
    };
    let tombstones = property_tags
        .iter()
        .map(|property_tag| canonical_property_storage_tag(*property_tag))
        .filter(|property_tag| is_lazy_folder_profile_property_tag(*property_tag))
        .collect::<Vec<_>>();
    if tombstones.is_empty() {
        return;
    }
    session
        .folder_profile_property_tombstones
        .entry(handle)
        .or_default()
        .extend(tombstones);
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
    snapshot: &mut MapiMailStoreSnapshot,
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
    let mut values = requested_values
        .into_iter()
        .map(|(tag, value)| (session.normalize_named_property_tag(tag), value))
        .collect::<Vec<_>>();
    let delegate_freebusy_mutation = matches!(
        set_properties_object.as_ref(),
        Some(MapiObject::DelegateFreeBusyMessage { .. })
    );
    if delegate_freebusy_mutation {
        // [MS-OXOPFFB] section 2.2.1.4.3: this deprecated property MUST be
        // ignored upon receipt, including in mixed SetProperties requests.
        values.retain(|(tag, _)| {
            canonical_property_storage_tag(*tag) != PID_TAG_SCHEDULE_INFO_FREE_BUSY
        });
    }
    let mut event_property_problems = Vec::new();
    let set_result = if delegate_freebusy_mutation && values.is_empty() {
        Ok(())
    } else if let Some(result) = stage_virtual_conversation_action_property_values(
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
            Some(MapiObject::Contact { .. }) => {
                stage_contact_property_values(session, handle_slots, request, snapshot, values)
            }
            Some(MapiObject::PendingContact { .. }) => {
                stage_pending_contact_property_values(session, handle_slots, request, values)
            }
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
            Some(MapiObject::DelegateFreeBusyMessage { .. }) => {
                // Outlook's provider-private named properties (for example
                // `fixupfbfolder`) are staged on this Message handle until a
                // successful SaveChangesMessage. They never replace canonical
                // grants or delegate preferences.
                stage_delegate_freebusy_property_values(
                    session,
                    handle_slots,
                    request,
                    snapshot,
                    values,
                )
            }
            Some(
                object @ (MapiObject::Task { .. }
                | MapiObject::Note { .. }
                | MapiObject::JournalEntry { .. }
                | MapiObject::ConversationAction { .. }
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
                async {
                    let aliases = default_folder_entry_id_aliases(object.as_ref(), &values);
                    let mut values = default_folder_identification_safe_property_values(
                        principal,
                        object.as_ref(),
                        values,
                    );
                    let Some(MapiObject::Folder { folder_id, .. }) = object else {
                        unreachable!("matched folder object")
                    };
                    if values.iter().any(|(tag, _)| {
                        canonical_property_storage_tag(*tag) == PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                    }) {
                        let folder_profile_values =
                            folder_profile_property_values(folder_id, &values)?;
                        let durable_inbox_folder_id = snapshot
                            .identity_codec()
                            .actual_object_id(INBOX_FOLDER_ID)
                            .ok_or_else(|| anyhow!("durable MAPI Inbox identity was not found"))?;
                        let committed = store
                            .commit_mapi_folder_hierarchy_property_values(
                                principal.account_id,
                                durable_inbox_folder_id,
                                &folder_profile_values,
                                &aliases,
                            )
                            .await?;
                        let committed_additional_ren_entry_ids = committed
                            .profile_values
                            .iter()
                            .find(|value| {
                                value.property_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                                    && value.property_type
                                        == (PID_TAG_ADDITIONAL_REN_ENTRY_IDS & 0xffff) as u16
                            })
                            .and_then(|value| {
                                additional_ren_entry_ids_from_profile_bytes(&value.property_value)
                            })
                            .ok_or_else(|| {
                                anyhow!("committed PidTagAdditionalRenEntryIds was not found")
                            })?;
                        let (_, additional_ren_entry_ids) = values
                            .iter_mut()
                            .find(|(tag, _)| {
                                canonical_property_storage_tag(*tag)
                                    == PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                            })
                            .ok_or_else(|| {
                                anyhow!("PidTagAdditionalRenEntryIds mutation was not found")
                            })?;
                        *additional_ren_entry_ids = committed_additional_ren_entry_ids;
                        let mut version = committed.version;
                        version.folder_id = INBOX_FOLDER_ID;
                        let change_number = version.change_number;
                        snapshot.upsert_folder_version(version);
                        for alias in &aliases {
                            session.record_special_folder_alias(
                                alias.alias_folder_id,
                                alias.canonical_folder_id,
                            );
                        }
                        apply_mapi_property_values(
                            input_object_mut(session, handle_slots, request),
                            values.clone(),
                        )?;
                        clear_folder_profile_property_tombstones(
                            session,
                            handle_slots,
                            request,
                            &values,
                        );
                        let mut event = MapiNotificationEvent::hierarchy(
                            IPM_SUBTREE_FOLDER_ID,
                            Some(INBOX_FOLDER_ID),
                        )
                        .with_object_kind("mailbox");
                        event.modseq = Some(change_number);
                        session.record_notification(event);
                        Ok(())
                    } else {
                        store
                            .upsert_mapi_special_folder_aliases(principal.account_id, &aliases)
                            .await?;
                        for alias in &aliases {
                            session.record_special_folder_alias(
                                alias.alias_folder_id,
                                alias.canonical_folder_id,
                            );
                        }
                        persist_profile_folder_property_values(
                            store, principal, folder_id, &values,
                        )
                        .await?;
                        apply_mapi_property_values(
                            input_object_mut(session, handle_slots, request),
                            values.clone(),
                        )?;
                        clear_folder_profile_property_tombstones(
                            session,
                            handle_slots,
                            request,
                            &values,
                        );
                        Ok(())
                    }
                }
                .await
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

fn stage_delegate_freebusy_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let Some(MapiObject::DelegateFreeBusyMessage {
        message_id,
        pending_appointment_tombstone,
        transaction,
        ..
    }) = input_object_mut(session, handle_slots, request)
    else {
        return Err(anyhow!("MAPI delegate free/busy message was not found"));
    };
    if !snapshot.is_outlook_local_freebusy_message_id(*message_id) {
        return Err(anyhow!(
            "unsupported delegate free/busy Message property mutation"
        ));
    }
    for (tag, value) in &values {
        if canonical_property_storage_tag(*tag) == PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE {
            if !matches!(value, MapiValue::Binary(_)) {
                return Err(anyhow!("appointment tombstone must be binary"));
            }
        } else if !is_custom_property_tag(*tag) {
            return Err(anyhow!(
                "unsupported delegate free/busy Message property mutation"
            ));
        }
    }
    for (tag, value) in values {
        if canonical_property_storage_tag(tag) == PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE {
            let MapiValue::Binary(value) = value else {
                unreachable!()
            };
            // [MS-OXCPRPT] sections 3.2.5.4 and 3.2.5.13: the new value is
            // immediately visible through this Message handle, while Save is
            // the publication boundary. The computed LocalFreebusy object
            // never stores a second copy outside canonical calendar state.
            *pending_appointment_tombstone = Some(value);
        } else {
            transaction.deleted_properties.remove(&tag);
            transaction.pending_properties.insert(tag, value);
        }
    }
    Ok(())
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
    let mut property_tags = requested_property_tags
        .iter()
        .copied()
        .into_iter()
        .map(|tag| session.normalize_named_property_tag(tag))
        .collect::<Vec<_>>();
    hydrate_folder_handle_properties_for_request(
        store,
        principal,
        session,
        handle_slots,
        request,
        &property_tags,
    )
    .await;
    let object = input_object(session, handle_slots, request).cloned();
    let delegate_freebusy_mutation = matches!(
        object.as_ref(),
        Some(MapiObject::DelegateFreeBusyMessage { .. })
    );
    if delegate_freebusy_mutation {
        // [MS-OXOPFFB] section 2.2.1.4.3 applies to deletes as well as sets;
        // filter the deprecated property and process any remaining tags.
        property_tags
            .retain(|tag| canonical_property_storage_tag(*tag) != PID_TAG_SCHEDULE_INFO_FREE_BUSY);
    }
    let mut event_property_problems = Vec::new();
    let delete_result = if delegate_freebusy_mutation && property_tags.is_empty() {
        Ok(())
    } else if let Some(result) = stage_virtual_conversation_action_property_delete(
        session,
        handle_slots,
        request,
        snapshot,
        &property_tags,
    ) {
        result
    } else if delegate_freebusy_mutation {
        stage_delegate_freebusy_property_deletions(
            session,
            handle_slots,
            request,
            snapshot,
            &property_tags,
        )
    } else if matches!(object, Some(MapiObject::Event { .. })) {
        stage_event_property_deletions(session, handle_slots, request, snapshot, &property_tags)
            .map(|problems| event_property_problems = problems)
    } else if matches!(object, Some(MapiObject::PendingEvent { .. })) {
        stage_pending_event_property_deletions(
            session,
            handle_slots,
            request,
            principal,
            &property_tags,
        )
        .map(|problems| event_property_problems = problems)
    } else if matches!(object, Some(MapiObject::Contact { .. })) {
        stage_contact_property_deletions(session, handle_slots, request, snapshot, &property_tags)
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
        ref saved_message,
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
    } else if matches!(object, Some(MapiObject::Message { .. })) {
        stage_message_property_deletions(session, handle_slots, request, &property_tags)
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
    if delete_result.is_ok() {
        mark_folder_profile_property_tombstones(
            session,
            handle_slots,
            request,
            object.as_ref(),
            &property_tags,
        );
    }
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

fn stage_delegate_freebusy_property_deletions(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<()> {
    let Some(MapiObject::DelegateFreeBusyMessage {
        message_id,
        transaction,
        ..
    }) = input_object_mut(session, handle_slots, request)
    else {
        return Err(anyhow!("MAPI delegate free/busy message was not found"));
    };
    if !snapshot.is_outlook_local_freebusy_message_id(*message_id)
        || property_tags
            .iter()
            .copied()
            .any(|tag| !is_custom_property_tag(tag))
    {
        return Err(anyhow!(
            "unsupported delegate free/busy Message property deletion"
        ));
    }
    for tag in property_tags {
        transaction.pending_properties.remove(tag);
        transaction.deleted_properties.insert(*tag);
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_profile_tombstones_are_handle_local_and_clear_on_set() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "user@example.test".to_string(),
            display_name: "User".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        let folder_handle = 7;
        let other_folder_handle = 8;
        let object = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };
        let request = RopRequest {
            rop_id: RopId::DeleteProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let handle_slots = [folder_handle];
        session.folder_profile_property_tombstones.insert(
            other_folder_handle,
            HashSet::from([PID_TAG_EXTENDED_FOLDER_FLAGS]),
        );

        mark_folder_profile_property_tombstones(
            &mut session,
            &handle_slots,
            &request,
            Some(&object),
            &[
                PID_TAG_EXTENDED_FOLDER_FLAGS,
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                PID_TAG_DISPLAY_NAME_W,
            ],
        );

        assert_eq!(
            session
                .folder_profile_property_tombstones
                .get(&folder_handle),
            Some(&HashSet::from([
                PID_TAG_EXTENDED_FOLDER_FLAGS,
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
            ]))
        );
        clear_folder_profile_property_tombstones(
            &mut session,
            &handle_slots,
            &request,
            &[(PID_TAG_EXTENDED_FOLDER_FLAGS, MapiValue::Binary(vec![1]))],
        );

        assert_eq!(
            session
                .folder_profile_property_tombstones
                .get(&folder_handle),
            Some(&HashSet::from([PID_TAG_ADDITIONAL_REN_ENTRY_IDS]))
        );
        assert_eq!(
            session
                .folder_profile_property_tombstones
                .get(&other_folder_handle),
            Some(&HashSet::from([PID_TAG_EXTENDED_FOLDER_FLAGS]))
        );
        clear_folder_profile_property_tombstones(
            &mut session,
            &handle_slots,
            &request,
            &[(
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                MapiValue::MultiBinary(Vec::new()),
            )],
        );

        assert!(!session
            .folder_profile_property_tombstones
            .contains_key(&folder_handle));
    }
}
