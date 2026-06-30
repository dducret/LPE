use super::*;

pub(super) fn conversation_action_properties(
    action: &lpe_storage::ConversationAction,
) -> HashMap<u32, MapiValue> {
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_CONVERSATION_INDEX,
        MapiValue::Binary(conversation_index_for_uuid(action.conversation_id)),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String(conversation_action_subject(action)),
    );
    if let Some(value) = &action.move_folder_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.move_store_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.max_delivery_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    if let Some(value) = &action.last_applied_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    properties.insert(
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        MapiValue::I32(action.version),
    );
    properties.insert(
        PID_LID_CONVERSATION_PROCESSED_TAG,
        MapiValue::I32(action.processed),
    );
    properties.insert(
        PID_NAME_KEYWORDS_TAG,
        MapiValue::MultiString(
            serde_json::from_str::<Vec<String>>(&action.categories_json).unwrap_or_default(),
        ),
    );
    properties
}

pub(super) async fn apply_conversation_action_to_existing_messages<S>(
    store: &S,
    principal: &AccountPrincipal,
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let categories = serde_json::from_str::<Vec<String>>(&action.categories_json)
        .unwrap_or_default()
        .into_iter()
        .map(|category| category.trim().to_string())
        .filter(|category| !category.is_empty())
        .collect::<Vec<_>>();
    let target_mailbox = if action.move_store_entry_id.is_some() {
        None
    } else {
        conversation_action_target_mailbox(action, mailboxes)
    };
    for email in emails
        .iter()
        .filter(|email| email.thread_id == action.conversation_id)
        .filter(|email| email.mailbox_role != "sent")
        .filter(|email| {
            action
                .max_delivery_time
                .as_deref()
                .map(|max_delivery| email.received_at.as_str() > max_delivery)
                .unwrap_or(true)
        })
    {
        if !categories.is_empty() && email.categories != categories {
            store
                .update_jmap_email_followup_flags(
                    principal.account_id,
                    email.id,
                    lpe_storage::JmapEmailFollowupUpdate {
                        categories: Some(categories.clone()),
                        ..Default::default()
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-conversation-action-categorize".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await?;
        }
        let Some(target_mailbox) = target_mailbox else {
            continue;
        };
        if email.mailbox_id == target_mailbox.id {
            continue;
        }
        store
            .move_jmap_email_from_mailbox(
                principal.account_id,
                email.mailbox_id,
                email.id,
                target_mailbox.id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-conversation-action-move".to_string(),
                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                },
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn apply_conversation_actions_to_new_message<S>(
    store: &S,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    email: &JmapEmail,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    for message in snapshot
        .conversation_action_messages()
        .iter()
        .filter(|message| message.action.conversation_id == email.thread_id)
    {
        apply_conversation_action_to_existing_messages(
            store,
            principal,
            &message.action,
            mailboxes,
            std::slice::from_ref(email),
        )
        .await?;
    }
    Ok(())
}

fn conversation_action_target_mailbox<'a>(
    action: &lpe_storage::ConversationAction,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    if action.move_store_entry_id.is_some() {
        return None;
    }
    if let Some(mailbox_id) = action.move_target_mailbox_id {
        return mailboxes.iter().find(|mailbox| mailbox.id == mailbox_id);
    }
    match action.move_folder_entry_id.as_deref() {
        Some([]) => mailboxes.iter().find(|mailbox| mailbox.role == "trash"),
        Some(entry_id) => {
            let folder_id = crate::mapi::identity::object_id_from_folder_entry_id(entry_id)?;
            folder_row_for_id(folder_id, mailboxes)
        }
        None => None,
    }
}

pub(super) fn conversation_action_target_mailbox_id(
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    conversation_action_target_mailbox(action, mailboxes).map(|mailbox| mailbox.id)
}

pub(super) async fn delete_conversation_action_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    conversation_action_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let existing = snapshot
        .conversation_action_message_for_id(conversation_action_id)
        .filter(|message| message.folder_id == folder_id)
        .ok_or_else(|| anyhow!("canonical MAPI conversation action was not found"))?;
    let mut properties = conversation_action_properties(&existing.action);
    for tag in property_tags {
        properties.remove(tag);
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    let action = conversation_action_from_mapi_properties(&properties);
    let move_target_mailbox_id = conversation_action_target_mailbox_id(&action, mailboxes);
    let saved = store
        .upsert_conversation_action(lpe_storage::UpsertConversationActionInput {
            account_id: principal.account_id,
            conversation_id: action.conversation_id,
            subject: action.subject,
            categories_json: action.categories_json,
            move_folder_entry_id: action.move_folder_entry_id,
            move_store_entry_id: action.move_store_entry_id,
            move_target_mailbox_id,
            max_delivery_time: action.max_delivery_time,
            last_applied_time: action.last_applied_time,
            version: Some(action.version),
            processed: Some(action.processed),
        })
        .await?;
    apply_conversation_action_to_existing_messages(store, principal, &saved, mailboxes, emails)
        .await
}

pub(super) fn stage_virtual_conversation_action_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Option<Result<()>> {
    let object = input_object_mut(session, handle_slots, request)?;
    let MapiObject::ConversationAction {
        folder_id,
        conversation_action_id,
    } = object
    else {
        return None;
    };
    if !crate::mapi_store::is_outlook_default_conversation_action_id(*conversation_action_id) {
        return None;
    }
    let Some(message) = snapshot
        .conversation_action_table_message_for_id(*conversation_action_id)
        .filter(|message| message.folder_id == *folder_id)
    else {
        return Some(Err(anyhow!(
            "virtual MAPI conversation action was not found"
        )));
    };
    let folder_id = *folder_id;
    let mut properties = conversation_action_properties(&message.action);
    apply_mapi_property_values_to_map(&mut properties, values);
    *object = MapiObject::PendingConversationAction {
        folder_id,
        properties,
    };
    Some(Ok(()))
}

pub(super) fn stage_virtual_conversation_action_property_delete(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Option<Result<()>> {
    let object = input_object_mut(session, handle_slots, request)?;
    let MapiObject::ConversationAction {
        folder_id,
        conversation_action_id,
    } = object
    else {
        return None;
    };
    if !crate::mapi_store::is_outlook_default_conversation_action_id(*conversation_action_id) {
        return None;
    }
    let Some(message) = snapshot
        .conversation_action_table_message_for_id(*conversation_action_id)
        .filter(|message| message.folder_id == *folder_id)
    else {
        return Some(Err(anyhow!(
            "virtual MAPI conversation action was not found"
        )));
    };
    let folder_id = *folder_id;
    let mut properties = conversation_action_properties(&message.action);
    for tag in property_tags {
        properties.remove(tag);
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    *object = MapiObject::PendingConversationAction {
        folder_id,
        properties,
    };
    Some(Ok(()))
}
